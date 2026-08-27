#!/usr/bin/env bash
# =============================================================================
# restore-cluster.sh - 從備份還原叢集骨架（topic 定義、設定、offset、ACL）
#
# 這支腳本「不會」還原訊息本體。訊息的復原有三條路：
#   a) 副本還在        -> 什麼都不用做，broker 自己會追上
#   b) 有 DR 叢集      -> 切流量過去（scripts/dr/failover.sh）
#   c) 有訊息層級備份  -> scripts/backup/backup-topic-data.sh 的還原模式
#
# 還原順序很重要（照這個順序做，才不會互相卡住）：
#   1. 目標叢集已啟動且健康
#   2. 還原 broker 動態設定
#   3. 重建 topic
#   4. 還原 topic 動態設定
#   5. 還原 ACL
#   6. （資料匯入，若有）
#   7. 還原 consumer group offset  <- 一定要在資料匯入之後
#
# 用法：
#   ./scripts/backup/restore-cluster.sh --from <備份目錄|tar.gz> --target host:9092
#   ./scripts/backup/restore-cluster.sh --from ... --target ... --dry-run
#   ./scripts/backup/restore-cluster.sh --from ... --target ... --skip-offsets
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

FROM=""
TARGET=""
SKIP_OFFSETS=false
SKIP_ACLS=false
TMP_EXTRACT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)         FROM="$2"; shift 2 ;;
    --target)       TARGET="$2"; shift 2 ;;
    --skip-offsets) SKIP_OFFSETS=true; shift ;;
    --skip-acls)    SKIP_ACLS=true; shift ;;
    --dry-run)      DRY_RUN=true; shift ;;
    -h|--help)      sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

[[ -n "${FROM}"   ]] || die "請用 --from 指定備份"
[[ -n "${TARGET}" ]] || die "請用 --target 指定目標叢集 bootstrap"

# 注意：EXIT trap 在 set -e 下若以非零狀態結束，會蓋掉腳本原本的退出碼，故補 return 0
cleanup() { [[ -n "${TMP_EXTRACT}" ]] && rm -rf "${TMP_EXTRACT}"; return 0; }
trap cleanup EXIT

if [[ -f "${FROM}" && "${FROM}" == *.tar.gz ]]; then
  TMP_EXTRACT="$(mktemp -d)"
  log_info "解開 ${FROM}"
  tar -xzf "${FROM}" -C "${TMP_EXTRACT}"
  FROM="$(find "${TMP_EXTRACT}" -maxdepth 1 -type d -name '2*Z' | head -1)"
fi
[[ -d "${FROM}" ]] || die "找不到備份目錄：${FROM}"

# 先驗證備份，別拿壞掉的備份去還原
section "還原前驗證備份"
"${REPO_ROOT}/scripts/backup/verify-backup.sh" "${FROM}" || die "備份驗證未通過，中止還原"

# -----------------------------------------------------------------------------
section "還原計畫"
BOOTSTRAP_SERVERS="${TARGET}"   # 之後所有 kafka_* 包裝都會打到目標叢集
cat >&2 <<EOF
  來源備份 : ${FROM}
  目標叢集 : ${TARGET}
  還原項目 : broker 設定 / topic / topic 設定$( [[ "${SKIP_ACLS}" == true ]] || echo ' / ACL')$( [[ "${SKIP_OFFSETS}" == true ]] || echo ' / consumer offset')
EOF
grep -E '^(backup_id|cluster_id|kafka_version|topic_count|group_count)=' "${FROM}/manifest.txt" | sed 's/^/  /' >&2

cluster_ready || die "目標叢集 ${TARGET} 無法連線，請先啟動"

# 目標叢集若已有使用者 topic，要特別小心
EXISTING="$(kafka_topics --list 2>/dev/null | grep -vc '^__' || true)"
if (( EXISTING > 0 )); then
  log_warn "目標叢集已有 ${EXISTING} 個使用者 topic。"
  log_warn "重建使用 --if-not-exists（既有 topic 的 partition/RF 不會被改動），"
  log_warn "但步驟 3/5 會把備份中的 topic 設定以 --alter 套用到既有 topic 上。"
fi
confirm "確定要對 ${TARGET} 執行還原嗎？" || die "已取消"

# -----------------------------------------------------------------------------
section "1/5 還原 broker 動態設定"
BROKER_CFG="${FROM}/config/broker-dynamic-configs.txt"
if [[ -f "${BROKER_CFG}" ]]; then
  # 只還原「明確設定過」的動態值（Dynamic ... config 標記），避免把預設值寫死
  RESTORED=0
  while IFS= read -r line; do
    # 範例： retention.ms=604800000 sensitive=false synonyms={DYNAMIC_BROKER_CONFIG:...}
    [[ "${line}" == *"DYNAMIC_BROKER_CONFIG"* ]] || continue
    # 抓「name=value」整段直到 sensitive= 標記為止——值可能含空白或逗號，
    # 不能用 awk '{print $1}'（那會把值截斷）
    KV="$(sed -nE 's/^ *([^ ]+=.*[^ ]) +sensitive=(true|false).*/\1/p;t;s/^ *([^ ]+=[^ ]*) +sensitive=(true|false).*/\1/p' <<<"${line}")"
    [[ "${KV}" == *=* ]] || continue
    # 敏感設定（密碼類）--describe 只會顯示 name=null，還原它會把密碼設成字串 "null"
    if [[ "${line}" == *" sensitive=true"* || "${KV#*=}" == "null" ]]; then
      log_warn "  略過敏感／不可讀取的設定：${KV%%=*}（請於還原後手動設定）"
      continue
    fi
    # 值含逗號時要用 [] 包起來，否則 --add-config 會把它拆成多個設定
    if [[ "${KV#*=}" == *,* ]]; then
      KV="${KV%%=*}=[${KV#*=}]"
    fi
    log_info "  broker config：${KV}"
    kafka_configs --entity-type brokers --entity-default --alter --add-config "${KV}" >/dev/null 2>&1 \
      && RESTORED=$(( RESTORED + 1 )) || log_warn "  設定失敗（可能為唯讀參數）：${KV}"
  done < "${BROKER_CFG}"
  log_ok "還原 ${RESTORED} 項 broker 動態設定"
else
  log_info "備份中沒有 broker 動態設定，略過"
fi

# -----------------------------------------------------------------------------
section "2/5 重建 topic"
if [[ "${DRY_RUN}" == "true" ]]; then
  log_info "[DRY-RUN] 會執行 ${FROM}/topics/recreate-topics.sh"
  sed -n '1,40p' "${FROM}/topics/recreate-topics.sh" | sed 's/^/    /' >&2
else
  # 先收集輸出、再過濾顯示：直接接在 pipeline 上的話，
  # (a) recreate 部分失敗會讓 pipefail 中斷整個還原（應該繼續其餘步驟）
  # (b) 零 topic 的備份輸出為空，grep -v 會以 exit 1 假失敗
  RECREATE_LOG="$(mktemp)"
  RECREATE_RC=0
  BOOTSTRAP_SERVERS="${TARGET}" KAFKA_HOME="${KAFKA_HOME}" \
    bash "${FROM}/topics/recreate-topics.sh" > "${RECREATE_LOG}" 2>&1 || RECREATE_RC=$?
  { grep -v 'Picked up' "${RECREATE_LOG}" || true; } | sed 's/^/  /' >&2
  rm -f "${RECREATE_LOG}"
  if (( RECREATE_RC == 0 )); then
    log_ok "topic 重建完成"
  else
    log_warn "部分 topic 建立失敗（見上方清單）；繼續進行其餘還原步驟"
  fi
fi

# -----------------------------------------------------------------------------
section "3/5 套用並核對 topic 設定"
# --if-not-exists 對「既有」topic 不會套用任何設定，
# 這裡把備份中的 topic 設定用 --alter 明確套上（新建的 topic 套用同值屬 no-op）
CFG_APPLIED=0
if [[ "${DRY_RUN}" != "true" ]]; then
  while IFS= read -r t; do
    [[ -z "${t}" || "${t}" == __* ]] && continue
    kafka_topics --describe --topic "${t}" >/dev/null 2>&1 || continue   # 建立失敗的跳過
    CFG="$(awk '/^Topic: /{for(i=1;i<=NF;i++) if($i=="Configs:") {print $(i+1); exit}}' \
             "${FROM}/topics/${t//\//_}.describe" 2>/dev/null || true)"
    [[ -n "${CFG}" && "${CFG}" != "Configs:" ]] || continue
    # 逗號只在「後面接 key= 」時才是設定分隔符；值內的逗號（如 1:0,2:0）要保留
    while IFS= read -r kv; do
      [[ -z "${kv}" || "${kv}" != *=* ]] && continue
      VAL="${kv#*=}"
      [[ "${VAL}" == *,* ]] && kv="${kv%%=*}=[${VAL}]"
      kafka_configs --entity-type topics --entity-name "${t}" --alter --add-config "${kv}" >/dev/null 2>&1 \
        && CFG_APPLIED=$(( CFG_APPLIED + 1 )) \
        || log_warn "  ${t}：套用 ${kv%%=*} 失敗"
    done < <(sed -E 's/,([a-zA-Z0-9._-]+=)/\n\1/g' <<<"${CFG}")
  done < "${FROM}/topics/topic-list.txt"
  log_ok "套用 ${CFG_APPLIED} 項 topic 設定"
fi

DIFF_COUNT=0
while IFS= read -r t; do
  [[ -z "${t}" || "${t}" == __* ]] && continue
  EXPECT="$(awk '/^Topic: /{print $6"/"$8; exit}' "${FROM}/topics/${t//\//_}.describe" 2>/dev/null || echo "")"
  ACTUAL="$(kafka_topics --describe --topic "${t}" 2>/dev/null | awk '/^Topic: /{print $6"/"$8; exit}')"
  if [[ "${EXPECT}" != "${ACTUAL}" ]]; then
    log_warn "  ${t}：partition/RF 預期 ${EXPECT:-<備份缺 describe>}，實際 ${ACTUAL:-<不存在>}"
    DIFF_COUNT=$(( DIFF_COUNT + 1 ))
  fi
done < "${FROM}/topics/topic-list.txt"
if (( DIFF_COUNT == 0 )); then
  log_ok "所有 topic 的 partition 與 RF 皆與備份一致"
else
  log_warn "${DIFF_COUNT} 個 topic 與備份不一致（若目標叢集 broker 數較少，RF 差異屬預期）"
fi

# -----------------------------------------------------------------------------
section "4/5 還原 ACL"
if [[ "${SKIP_ACLS}" == "true" ]]; then
  log_info "--skip-acls，略過"
elif [[ -f "${FROM}/acls/acls.txt" ]] && grep -q 'principal' "${FROM}/acls/acls.txt" 2>/dev/null; then
  log_warn "ACL 需要人工確認後套用。備份內容："
  sed 's/^/    /' "${FROM}/acls/acls.txt" >&2
  log_warn "請依上述內容用 kafka-acls.sh --add 逐條建立（避免自動化誤放權限）"
else
  log_info "備份中沒有 ACL，略過"
fi

# -----------------------------------------------------------------------------
section "5/5 還原 consumer group offset"
if [[ "${SKIP_OFFSETS}" == "true" ]]; then
  log_info "--skip-offsets，略過"
elif [[ -f "${FROM}/groups/restore-offsets.sh" ]]; then
  log_warn "還原 offset 的前提：目標 topic 的資料已經就位，且該 group 沒有 consumer 在跑。"
  log_warn "若資料尚未匯入就還原 offset，consumer 會跳過還沒到的訊息。"
  if confirm "資料是否已就位、consumer 是否都已停止？"; then
    if [[ "${DRY_RUN}" == "true" ]]; then
      log_info "[DRY-RUN] 會執行 restore-offsets.sh"
    else
      OFFSET_LOG="$(mktemp)"
      OFFSET_RC=0
      BOOTSTRAP_SERVERS="${TARGET}" KAFKA_HOME="${KAFKA_HOME}" \
        bash "${FROM}/groups/restore-offsets.sh" > "${OFFSET_LOG}" 2>&1 || OFFSET_RC=$?
      { grep -v 'Picked up' "${OFFSET_LOG}" || true; } | sed 's/^/  /' >&2
      rm -f "${OFFSET_LOG}"
      if (( OFFSET_RC == 0 )); then
        log_ok "offset 還原完成"
      else
        log_warn "部分 group 還原失敗（見上方清單；可能 topic 不存在或 offset 超出範圍）"
      fi
    fi
  else
    log_info "已略過 offset 還原。稍後可單獨執行："
    printf '    BOOTSTRAP_SERVERS=%s bash %s/groups/restore-offsets.sh\n' "${TARGET}" "${FROM}" >&2
  fi
else
  log_info "備份中沒有 offset 資料，略過"
fi

# -----------------------------------------------------------------------------
section "還原後健康檢查"
BOOTSTRAP_SERVERS="${TARGET}" "${REPO_ROOT}/scripts/ops/health-check.sh" || true

section "還原完成"
cat >&2 <<EOF
  提醒：
    1. 這次還原的是「叢集骨架」，訊息本體不在其中。
    2. 請確認 producer / consumer 的 bootstrap.servers 已指向 ${TARGET}。
    3. 若這是災難切換，請把切換時間、資料落差記錄到事件報告中。
EOF
