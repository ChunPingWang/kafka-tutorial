#!/usr/bin/env bash
# =============================================================================
# dr-status.sh - 災難備援健康度儀表板
#
# 回答四個問題：
#   1. 複寫還活著嗎？        （heartbeats topic 的最新時間戳）
#   2. 落後多少？            （每個 topic 的 offset 差 = 潛在資料落差 / RPO）
#   3. consumer offset 同步了嗎？（切過去之後 consumer 會從哪裡開始）
#   4. 現在切過去，會遺失多少？  （估算 RPO）
#
# 用法：
#   ./scripts/dr/dr-status.sh --source-alias dc1 --source localhost:9092 --target localhost:19092
#   ./scripts/dr/dr-status.sh --auto      # 從 mm2/topology 讀取設定
#   ./scripts/dr/dr-status.sh --auto --format json
#
# 退出碼：0 = 健康，1 = 落後超過門檻，2 = 複寫中斷
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

SRC_ALIAS=""
SRC=""
DST=""
FORMAT="text"
LAG_WARN="${DR_LAG_WARN:-1000}"          # 訊息落差門檻
HEARTBEAT_WARN="${DR_HEARTBEAT_WARN:-60}" # 心跳延遲秒數門檻
IDENTITY_POLICY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-alias) SRC_ALIAS="$2"; shift 2 ;;
    --source)       SRC="$2"; shift 2 ;;
    --target)       DST="$2"; shift 2 ;;
    --format)       FORMAT="$2"; shift 2 ;;
    --auto)
      TOPO="${KAFKA_BASE_DIR}/mm2/topology"
      [[ -f "${TOPO}" ]] || die "找不到 ${TOPO}，請先執行 setup-mirrormaker.sh"
      # shellcheck disable=SC1090
      source "${TOPO}"
      SRC_ALIAS="${SOURCE_ALIAS}"; SRC="${SOURCE_BOOTSTRAP}"; DST="${TARGET_BOOTSTRAP}"
      shift ;;
    -h|--help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

[[ -n "${SRC}" && -n "${DST}" && -n "${SRC_ALIAS}" ]] \
  || die "請提供 --source-alias / --source / --target，或使用 --auto"

KT="${KAFKA_HOME}/bin/kafka-topics.sh"
KGO="${KAFKA_HOME}/bin/kafka-get-offsets.sh"
KCG="${KAFKA_HOME}/bin/kafka-consumer-groups.sh"
KCC="${KAFKA_HOME}/bin/kafka-console-consumer.sh"

SEVERITY=0
bump() { (( SEVERITY < $1 )) && SEVERITY="$1"; return 0; }

# 目標端對應的 topic 名稱
remote_name() {
  if [[ "${IDENTITY_POLICY}" == "true" ]]; then printf '%s' "$1"; else printf '%s.%s' "${SRC_ALIAS}" "$1"; fi
}

section "災難備援狀態  ${SRC_ALIAS}（${SRC}） -> （${DST}）"

# -----------------------------------------------------------------------------
# 0. 兩端連線
# -----------------------------------------------------------------------------
SRC_UP=false; DST_UP=false
BOOTSTRAP_SERVERS="${SRC}" cluster_ready && SRC_UP=true
BOOTSTRAP_SERVERS="${DST}" cluster_ready && DST_UP=true

printf '  來源叢集 : %s\n' "$( [[ "${SRC_UP}" == true ]] && printf '%s可連線%s' "${C_GRN}" "${C_RESET}" || printf '%s無法連線%s' "${C_RED}" "${C_RESET}")"
printf '  目標叢集 : %s\n' "$( [[ "${DST_UP}" == true ]] && printf '%s可連線%s' "${C_GRN}" "${C_RESET}" || printf '%s無法連線%s' "${C_RED}" "${C_RESET}")"

if [[ "${DST_UP}" != true ]]; then
  log_error "備援叢集無法連線 —— 現在發生災難將無處可切"
  exit 2
fi
if [[ "${SRC_UP}" != true ]]; then
  log_warn "來源叢集無法連線。若這是真實災難，請執行 scripts/dr/failover.sh"
  bump 2
fi

# -----------------------------------------------------------------------------
# 1. 心跳：複寫還活著嗎
# -----------------------------------------------------------------------------
section "1. 複寫心跳"
HB_TOPIC="$(remote_name heartbeats)"
if "${KT}" --bootstrap-server "${DST}" --list 2>/dev/null | grep -qx "${HB_TOPIC}"; then
  # 抓最後一筆心跳的時間戳
  LAST_HB="$("${KCC}" --bootstrap-server "${DST}" --topic "${HB_TOPIC}" \
      --max-messages 1 --timeout-ms 15000 \
      --property print.timestamp=true --property print.value=false \
      --offset latest --partition 0 2>/dev/null \
      | sed -n 's/^CreateTime:\([0-9]*\).*/\1/p' | tail -1 || true)"
  if [[ -z "${LAST_HB}" ]]; then
    # latest 可能還沒有新訊息，退而求其次抓 end offset 前一筆
    END="$("${KGO}" --bootstrap-server "${DST}" --topic "${HB_TOPIC}" --partitions 0 2>/dev/null | awk -F: '{print $3}')"
    if [[ -n "${END}" ]] && (( END > 0 )); then
      LAST_HB="$("${KCC}" --bootstrap-server "${DST}" --topic "${HB_TOPIC}" \
          --partition 0 --offset $(( END - 1 )) --max-messages 1 --timeout-ms 15000 \
          --property print.timestamp=true --property print.value=false 2>/dev/null \
          | sed -n 's/^CreateTime:\([0-9]*\).*/\1/p' | tail -1 || true)"
    fi
  fi
  if [[ -n "${LAST_HB}" ]]; then
    NOW_MS=$(( $(date +%s) * 1000 ))
    AGE_S=$(( (NOW_MS - LAST_HB) / 1000 ))
    if (( AGE_S <= HEARTBEAT_WARN )); then
      printf '  %s✔%s 最後心跳於 %d 秒前\n' "${C_GRN}" "${C_RESET}" "${AGE_S}"
    else
      printf '  %s✘%s 最後心跳於 %d 秒前（門檻 %ds）—— 複寫可能已中斷\n' "${C_RED}" "${C_RESET}" "${AGE_S}" "${HEARTBEAT_WARN}"
      bump 2
    fi
  else
    printf '  %s!%s 讀不到心跳訊息\n' "${C_YEL}" "${C_RESET}"
    bump 1
  fi
else
  printf '  %s✘%s 目標端沒有 %s topic —— MM2 沒有在跑？\n' "${C_RED}" "${C_RESET}" "${HB_TOPIC}"
  bump 2
fi

# -----------------------------------------------------------------------------
# 2. 每個 topic 的複寫落差
# -----------------------------------------------------------------------------
section "2. Topic 複寫落差（潛在資料遺失量）"
TOTAL_LAG=0
REPLICATED=0
MISSING=""

if [[ "${SRC_UP}" == true ]]; then
  printf '  %-32s %12s %12s %10s\n' "TOPIC" "來源" "目標" "落差"
  while IFS= read -r t; do
    [[ -z "${t}" || "${t}" == __* || "${t}" == mm2-* || "${t}" == heartbeats ]] && continue
    [[ "${t}" == *".internal" ]] && continue
    RT="$(remote_name "${t}")"

    S_OFF="$("${KGO}" --bootstrap-server "${SRC}" --topic "${t}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
    if "${KT}" --bootstrap-server "${DST}" --list 2>/dev/null | grep -qx "${RT}"; then
      D_OFF="$("${KGO}" --bootstrap-server "${DST}" --topic "${RT}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
      LAG=$(( S_OFF - D_OFF ))
      (( LAG < 0 )) && LAG=0
      TOTAL_LAG=$(( TOTAL_LAG + LAG ))
      REPLICATED=$(( REPLICATED + 1 ))
      MARK="${C_GRN}✔${C_RESET}"
      (( LAG > LAG_WARN )) && { MARK="${C_YEL}!${C_RESET}"; bump 1; }
      printf '  %b %-30s %12s %12s %10s\n' "${MARK}" "${t}" "${S_OFF}" "${D_OFF}" "${LAG}"
    else
      MISSING="${MISSING} ${t}"
      printf '  %b %-30s %12s %12s %10s\n' "${C_RED}✘${C_RESET}" "${t}" "${S_OFF}" "-" "未複寫"
      bump 1
    fi
  done < <("${KT}" --bootstrap-server "${SRC}" --list 2>/dev/null | sort)

  printf '\n  已複寫 %d 個 topic，總落差 %d 筆\n' "${REPLICATED}" "${TOTAL_LAG}"
  [[ -n "${MISSING}" ]] && printf '  %s未複寫：%s%s\n' "${C_RED}" "${MISSING# }" "${C_RESET}"
else
  printf '  來源不可連線，無法比對落差\n'
fi

# -----------------------------------------------------------------------------
# 3. Consumer offset 同步
# -----------------------------------------------------------------------------
section "3. Consumer group offset 同步狀態"
DST_GROUPS="$("${KCG}" --bootstrap-server "${DST}" --list 2>/dev/null | grep -v '^$' || true)"
SYNCED=0
if [[ -n "${DST_GROUPS}" ]]; then
  while IFS= read -r g; do
    [[ -z "${g}" || "${g}" == console-consumer-* || "${g}" == connect-* ]] && continue
    OFF_SUM="$("${KCG}" --bootstrap-server "${DST}" --describe --group "${g}" 2>/dev/null \
      | awk '$4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}')"
    if (( OFF_SUM > 0 )); then
      printf '  %s✔%s %-30s 已同步 offset 總和=%s\n' "${C_GRN}" "${C_RESET}" "${g}" "${OFF_SUM}"
      SYNCED=$(( SYNCED + 1 ))
    else
      printf '  %s·%s %-30s offset=0（尚未同步或本來就在開頭）\n' "${C_DIM}" "${C_RESET}" "${g}"
    fi
  done <<<"${DST_GROUPS}"
fi
if (( SYNCED == 0 )); then
  printf '  %s!%s 目標端沒有任何已同步 offset 的 group。\n' "${C_YEL}" "${C_RESET}"
  printf '    確認 MM2 設定有 sync.group.offsets.enabled = true，\n'
  printf '    且目標端該 group 沒有 active consumer（有的話 MM2 不會覆寫）。\n'
  bump 1
fi

# -----------------------------------------------------------------------------
# 4. RPO 評估
# -----------------------------------------------------------------------------
section "4. RPO 評估（現在切換會失去什麼）"
if [[ "${SRC_UP}" == true ]]; then
  printf '  未複寫訊息數 : %d 筆\n' "${TOTAL_LAG}"
  if (( TOTAL_LAG == 0 )); then
    printf '  %s現在切換不會遺失已複寫 topic 的資料。%s\n' "${C_GRN}" "${C_RESET}"
  else
    printf '  %s現在切換會遺失最多 %d 筆訊息。%s\n' "${C_YEL}" "${TOTAL_LAG}" "${C_RESET}"
  fi
else
  printf '  來源已不可連線，實際落差無法測得。\n'
  printf '  請以「最後一次心跳時間」推估：災難發生點之後的訊息都沒有複寫過來。\n'
fi

# -----------------------------------------------------------------------------
section "整體評估"
case "${SEVERITY}" in
  0) printf '  %s備援就緒：可以切換%s\n' "${C_GRN}" "${C_RESET}" ;;
  1) printf '  %s備援可用但有落差：切換前先看上面的警告%s\n' "${C_YEL}" "${C_RESET}" ;;
  2) printf '  %s備援不可靠：複寫中斷或叢集不可連線%s\n' "${C_RED}" "${C_RESET}" ;;
esac
exit "${SEVERITY}"
