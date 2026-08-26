#!/usr/bin/env bash
# =============================================================================
# topic-admin.sh - Topic 日常管理（建立 / 查詢 / 擴充 / 設定 / 刪除）
#
# 用法：
#   ./scripts/ops/topic-admin.sh <子指令> [參數]
#
# 子指令：
#   list                              列出所有 topic（含 partition/RF 摘要）
#   describe <topic>                  顯示 topic 詳細資訊與設定
#   create <topic> [-p N] [-r N] [-c k=v]...
#                                     建立 topic（會擋掉危險組合）
#   add-partitions <topic> <總數>     擴充 partition（不可縮減！）
#   set-config <topic> <k=v>...       修改 topic 設定
#   del-config <topic> <key>...       移除 topic 設定（回到 broker 預設）
#   delete <topic>                    刪除 topic（需二次確認）
#   lag <group>                       顯示 consumer group lag
#   groups                            列出所有 consumer group
#   reset-offset <group> <topic> <目標>
#                                     目標：earliest | latest | <timestamp ISO8601>
#   biggest [N]                       列出佔用空間最大的 N 個 topic
#
# 範例：
#   ./scripts/ops/topic-admin.sh create orders -p 12 -r 3 -c retention.ms=604800000
#   ./scripts/ops/topic-admin.sh reset-offset my-group orders earliest
#   ./scripts/ops/topic-admin.sh reset-offset my-group orders 2026-08-01T00:00:00.000
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

usage() { sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }
[[ $# -eq 0 ]] && usage 1

SUBCMD="$1"; shift

case "${SUBCMD}" in

  list)
    section "Topic 清單"
    kafka_topics --describe 2>/dev/null \
      | awk '/^Topic: /{printf "  %-40s partitions=%-4s RF=%-3s\n", $2, $6, $8}' \
      | sort
    printf '\n  共 %s 個 topic\n' "$(kafka_topics --list 2>/dev/null | grep -c . || true)"
    ;;

  describe)
    [[ $# -ge 1 ]] || die "用法：describe <topic>"
    section "Topic：$1"
    kafka_topics --describe --topic "$1"
    printf '\n'
    section "動態設定（非預設值）"
    kafka_configs --entity-type topics --entity-name "$1" --describe
    ;;

  create)
    [[ $# -ge 1 ]] || die "用法：create <topic> [-p N] [-r N] [-c k=v]..."
    TOPIC="$1"; shift
    PARTS=6 ; RF=3 ; declare -a CONFIGS=()
    while [[ $# -gt 0 ]]; do
      case "$1" in
        -p) PARTS="$2"; shift 2 ;;
        -r) RF="$2"; shift 2 ;;
        -c) CONFIGS+=(--config "$2"); shift 2 ;;
        *) die "未知選項：$1" ;;
      esac
    done

    # 安全檢查：RF 不可超過 broker 數
    BROKERS="$("$(kafka_bin kafka-broker-api-versions.sh)" \
        --bootstrap-server "${BOOTSTRAP_SERVERS}" 2>/dev/null \
        | grep -cE '^\S+:[0-9]+ \(id:' || true)"
    (( BROKERS < 1 )) && BROKERS=1
    if (( RF > BROKERS )); then
      die "replication-factor=${RF} 大於 broker 數 ${BROKERS}，建立會失敗"
    fi
    if (( RF < 3 )) && (( BROKERS >= 3 )); then
      log_warn "叢集有 ${BROKERS} 個 broker 卻只設 RF=${RF}，正式資料建議 RF=3"
      confirm "仍要繼續嗎？" || exit 1
    fi
    # partition 只能加不能減，先提醒
    log_info "提醒：partition 數只能增加、不能減少，請一次規劃到位（見 README「容量規劃」）"

    section "建立 topic ${TOPIC}"
    kafka_topics --create --topic "${TOPIC}" \
      --partitions "${PARTS}" --replication-factor "${RF}" "${CONFIGS[@]+"${CONFIGS[@]}"}"
    kafka_topics --describe --topic "${TOPIC}"
    ;;

  add-partitions)
    [[ $# -eq 2 ]] || die "用法：add-partitions <topic> <擴充後的總數>"
    TOPIC="$1"; NEW="$2"
    CUR="$(kafka_topics --describe --topic "${TOPIC}" 2>/dev/null | awk '/^Topic: /{print $6}')"
    [[ -n "${CUR}" ]] || die "找不到 topic ${TOPIC}"
    (( NEW > CUR )) || die "新的 partition 數 ${NEW} 必須大於目前的 ${CUR}（partition 不可縮減）"
    log_warn "重要：擴充 partition 會改變 key 的雜湊落點。"
    log_warn "同一個 key 之後可能落到不同 partition，跨 partition 的順序保證會斷掉。"
    log_warn "若業務仰賴 key 順序，正確做法是「建新 topic + 重新導流」，而不是擴充。"
    confirm "了解風險並確定擴充 ${TOPIC}：${CUR} -> ${NEW}？" || exit 1
    kafka_topics --alter --topic "${TOPIC}" --partitions "${NEW}"
    kafka_topics --describe --topic "${TOPIC}"
    ;;

  set-config)
    [[ $# -ge 2 ]] || die "用法：set-config <topic> <k=v>..."
    TOPIC="$1"; shift
    JOINED="$(IFS=,; echo "$*")"
    section "設定 ${TOPIC}：${JOINED}"
    kafka_configs --entity-type topics --entity-name "${TOPIC}" --alter --add-config "${JOINED}"
    kafka_configs --entity-type topics --entity-name "${TOPIC}" --describe
    ;;

  del-config)
    [[ $# -ge 2 ]] || die "用法：del-config <topic> <key>..."
    TOPIC="$1"; shift
    JOINED="$(IFS=,; echo "$*")"
    kafka_configs --entity-type topics --entity-name "${TOPIC}" --alter --delete-config "${JOINED}"
    kafka_configs --entity-type topics --entity-name "${TOPIC}" --describe
    ;;

  delete)
    [[ $# -eq 1 ]] || die "用法：delete <topic>"
    TOPIC="$1"
    section "準備刪除 ${TOPIC}"
    kafka_topics --describe --topic "${TOPIC}" || die "找不到 topic"
    log_warn "刪除後資料無法復原。若只是想清空，請改用 retention.ms=1000 等資料過期後再改回。"
    if [[ "${ASSUME_YES:-false}" != "true" ]]; then
      read -r -p "請輸入 topic 名稱以確認刪除：" TYPED
      [[ "${TYPED}" == "${TOPIC}" ]] || die "輸入不符，已取消"
    fi
    kafka_topics --delete --topic "${TOPIC}"
    log_ok "已送出刪除指令（實際刪除為非同步）"
    ;;

  lag)
    [[ $# -eq 1 ]] || die "用法：lag <group>"
    section "Consumer group：$1"
    kafka_groups --describe --group "$1"
    printf '\n'
    TOTAL="$(kafka_groups --describe --group "$1" 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}')"
    printf '  總 lag：%s\n' "${TOTAL}"
    ;;

  groups)
    section "Consumer groups"
    kafka_groups --list 2>/dev/null | sort | sed 's/^/  /'
    ;;

  reset-offset)
    [[ $# -eq 3 ]] || die "用法：reset-offset <group> <topic> <earliest|latest|ISO8601>"
    G="$1"; T="$2"; TARGET="$3"
    case "${TARGET}" in
      earliest) OPT=(--to-earliest) ;;
      latest)   OPT=(--to-latest) ;;
      *)        OPT=(--to-datetime "${TARGET}") ;;
    esac
    log_warn "重設 offset 前，該 group 的所有 consumer 必須先停止，否則指令會被拒絕。"
    section "預演（dry-run）"
    kafka_groups --group "${G}" --topic "${T}" --reset-offsets "${OPT[@]}" --dry-run
    confirm "以上是預演結果，確定要實際套用嗎？" || exit 1
    kafka_groups --group "${G}" --topic "${T}" --reset-offsets "${OPT[@]}" --execute
    log_ok "已重設，重新啟動 consumer 即會從新位置開始消費"
    ;;

  biggest)
    N="${1:-10}"
    command -v jq >/dev/null 2>&1 || die "此子指令需要 jq"
    section "佔用空間最大的 ${N} 個 topic"
    kafka_logdirs --describe 2>/dev/null | grep -E '^\{' | tail -1 | jq -r '
      [ .brokers[].logDirs[].partitions[] ]
      | map({topic: (.partition | sub("-[0-9]+$"; "")), size: .size})
      | group_by(.topic)
      | map({topic: .[0].topic, size: (map(.size) | add)})
      | sort_by(-.size)[]
      | "\(.size)\t\(.topic)"' \
      | head -n "${N}" \
      | while IFS=$'\t' read -r sz tp; do
          printf '  %-12s %s\n' "$(human_bytes "${sz}")" "${tp}"
        done
    ;;

  -h|--help|help) usage 0 ;;
  *) log_error "未知子指令：${SUBCMD}"; usage 1 ;;
esac
