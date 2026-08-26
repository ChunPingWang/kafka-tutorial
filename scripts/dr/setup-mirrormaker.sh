#!/usr/bin/env bash
# =============================================================================
# setup-mirrormaker.sh - 建立跨叢集複寫（MirrorMaker 2）
#
# 這是災難備援「資料層」的核心：把主叢集的訊息與 consumer offset
# 持續複寫到備援叢集，讓主叢集整座掛掉時還有東西可以切。
#
# 用法：
#   ./scripts/dr/setup-mirrormaker.sh \
#       --source-alias dc1 --source localhost:9092 \
#       --target-alias dc2 --target localhost:19092 \
#       --start
#
# 選項：
#   --source-alias NAME   來源叢集別名（會成為 topic 前綴）
#   --source HOST:PORT    來源 bootstrap
#   --target-alias NAME   目標叢集別名
#   --target HOST:PORT    目標 bootstrap
#   --topics REGEX        要複寫的 topic（預設 .* 但排除內部 topic）
#   --groups REGEX        要複寫 offset 的 group（預設 .*）
#   --bidirectional       雙向複寫（雙活架構）
#   --identity-policy     目標端保留原 topic 名（不加前綴，不可與雙向並用）
#   --rf N                MM2 內部 topic 的副本數（預設 3，單機測試用 1）
#   --tasks N             tasks.max（預設 4）
#   --start               產生設定後直接啟動
#   --config-only         只產生設定檔
#
# 啟動後如何確認：
#   ./scripts/dr/dr-status.sh --source-alias dc1 --target localhost:19092
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

SOURCE_ALIAS="${SOURCE_ALIAS:-primary}"
TARGET_ALIAS="${TARGET_ALIAS:-dr}"
SOURCE_BOOTSTRAP=""
TARGET_BOOTSTRAP=""
TOPICS_PATTERN=".*"
GROUPS_PATTERN=".*"
BIDIRECTIONAL=false
IDENTITY_POLICY=false
MM2_RF=3
TASKS_MAX=4
CHECKPOINT_INTERVAL=10
SYNC_ACLS=false
DO_START=false
CONFIG_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-alias)   SOURCE_ALIAS="$2"; shift 2 ;;
    --source)         SOURCE_BOOTSTRAP="$2"; shift 2 ;;
    --target-alias)   TARGET_ALIAS="$2"; shift 2 ;;
    --target)         TARGET_BOOTSTRAP="$2"; shift 2 ;;
    --topics)         TOPICS_PATTERN="$2"; shift 2 ;;
    --groups)         GROUPS_PATTERN="$2"; shift 2 ;;
    --bidirectional)  BIDIRECTIONAL=true; shift ;;
    --identity-policy) IDENTITY_POLICY=true; shift ;;
    --rf)             MM2_RF="$2"; shift 2 ;;
    --tasks)          TASKS_MAX="$2"; shift 2 ;;
    --sync-acls)      SYNC_ACLS=true; shift ;;
    --start)          DO_START=true; shift ;;
    --config-only)    CONFIG_ONLY=true; shift ;;
    -h|--help)        sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

[[ -n "${SOURCE_BOOTSTRAP}" ]] || die "請用 --source 指定來源叢集"
[[ -n "${TARGET_BOOTSTRAP}" ]] || die "請用 --target 指定目標叢集"
[[ "${SOURCE_ALIAS}" != "${TARGET_ALIAS}" ]] || die "來源與目標別名不可相同"
if [[ "${BIDIRECTIONAL}" == "true" && "${IDENTITY_POLICY}" == "true" ]]; then
  die "IdentityReplicationPolicy 不能用在雙向複寫（會造成訊息無窮迴圈）"
fi

# -----------------------------------------------------------------------------
section "檢查兩座叢集"
check_cluster() {
  local name="$1" bs="$2"
  if BOOTSTRAP_SERVERS="${bs}" cluster_ready; then
    local n
    n="$("${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server "${bs}" 2>/dev/null \
        | grep -cE '^\S+:[0-9]+ \(id:' || true)"
    log_ok "${name}（${bs}）可連線，${n} 個 broker"
    if (( MM2_RF > n )); then
      log_warn "${name} 只有 ${n} 個 broker，但 --rf=${MM2_RF}；MM2 內部 topic 會建立失敗"
      log_warn "測試環境請加 --rf ${n}"
    fi
  else
    die "${name}（${bs}）無法連線"
  fi
}
check_cluster "來源 ${SOURCE_ALIAS}" "${SOURCE_BOOTSTRAP}"
check_cluster "目標 ${TARGET_ALIAS}" "${TARGET_BOOTSTRAP}"

# -----------------------------------------------------------------------------
section "產生 MM2 設定"
MM2_DIR="${KAFKA_BASE_DIR}/mm2"
mkdir -p "${MM2_DIR}" "${KAFKA_LOG_DIR}"
MM2_CONF="${MM2_DIR}/mm2-${SOURCE_ALIAS}-to-${TARGET_ALIAS}.properties"

if [[ "${IDENTITY_POLICY}" == "true" ]]; then
  POLICY_LINE="replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy"
  log_info "使用 IdentityReplicationPolicy：目標端 topic 保持原名"
else
  POLICY_LINE="# 使用預設 DefaultReplicationPolicy：目標端 topic 名稱為 ${SOURCE_ALIAS}.<原名>"
  log_info "使用 DefaultReplicationPolicy：目標端 topic 會是 ${SOURCE_ALIAS}.<原名>"
fi

TMPL="${REPO_ROOT}/conf/templates/mm2.properties.tmpl"
[[ -f "${TMPL}" ]] || die "找不到範本 ${TMPL}"

sed \
  -e "s|@@SOURCE_ALIAS@@|${SOURCE_ALIAS}|g" \
  -e "s|@@TARGET_ALIAS@@|${TARGET_ALIAS}|g" \
  -e "s|@@SOURCE_BOOTSTRAP@@|${SOURCE_BOOTSTRAP}|g" \
  -e "s|@@TARGET_BOOTSTRAP@@|${TARGET_BOOTSTRAP}|g" \
  -e "s|@@TOPICS_PATTERN@@|${TOPICS_PATTERN}|g" \
  -e "s|@@GROUPS_PATTERN@@|${GROUPS_PATTERN}|g" \
  -e "s|@@REVERSE_ENABLED@@|${BIDIRECTIONAL}|g" \
  -e "s|@@REPLICATION_POLICY_LINE@@|${POLICY_LINE}|g" \
  -e "s|@@REPLICATION_FACTOR@@|${MM2_RF}|g" \
  -e "s|@@TASKS_MAX@@|${TASKS_MAX}|g" \
  -e "s|@@CHECKPOINT_INTERVAL@@|${CHECKPOINT_INTERVAL}|g" \
  -e "s|@@SYNC_ACLS@@|${SYNC_ACLS}|g" \
  "${TMPL}" > "${MM2_CONF}"

log_ok "已產生 ${MM2_CONF}"

# 記錄拓撲，供 failover / dr-status 使用
cat > "${MM2_DIR}/topology" <<EOF
SOURCE_ALIAS=${SOURCE_ALIAS}
SOURCE_BOOTSTRAP=${SOURCE_BOOTSTRAP}
TARGET_ALIAS=${TARGET_ALIAS}
TARGET_BOOTSTRAP=${TARGET_BOOTSTRAP}
BIDIRECTIONAL=${BIDIRECTIONAL}
IDENTITY_POLICY=${IDENTITY_POLICY}
CONFIG=${MM2_CONF}
CREATED_AT=$(timestamp)
EOF

# -----------------------------------------------------------------------------
if [[ "${CONFIG_ONLY}" == "true" ]]; then
  section "只產生設定（--config-only）"
  printf '  啟動指令：\n    %s/bin/connect-mirror-maker.sh %s\n' "${KAFKA_HOME}" "${MM2_CONF}" >&2
  exit 0
fi

if [[ "${DO_START}" != "true" ]]; then
  section "設定完成"
  printf '  用 --start 直接啟動，或手動執行：\n    %s/bin/connect-mirror-maker.sh %s\n' \
    "${KAFKA_HOME}" "${MM2_CONF}" >&2
  exit 0
fi

# -----------------------------------------------------------------------------
section "啟動 MirrorMaker 2"
MM2_LOG="${KAFKA_LOG_DIR}/mm2-${SOURCE_ALIAS}-to-${TARGET_ALIAS}.log"
MM2_PID_FILE="${MM2_DIR}/mm2-${SOURCE_ALIAS}-to-${TARGET_ALIAS}.pid"

if [[ -f "${MM2_PID_FILE}" ]] && kill -0 "$(cat "${MM2_PID_FILE}")" 2>/dev/null; then
  log_warn "MM2 已在執行（pid $(cat "${MM2_PID_FILE}")）"
  exit 0
fi

KAFKA_HEAP_OPTS="${MM2_HEAP_OPTS:--Xmx1G -Xms1G}" \
LOG_DIR="${KAFKA_LOG_DIR}" \
  nohup "${KAFKA_HOME}/bin/connect-mirror-maker.sh" "${MM2_CONF}" > "${MM2_LOG}" 2>&1 &
echo $! > "${MM2_PID_FILE}"
log_info "已啟動（pid $(cat "${MM2_PID_FILE}")），log：${MM2_LOG}"

# 等待第一批複寫 topic 出現在目標端
log_info "等待複寫開始（最多 120 秒）..."
WAITED=0
while (( WAITED < 120 )); do
  if "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${TARGET_BOOTSTRAP}" --list 2>/dev/null \
       | grep -qE "^(${SOURCE_ALIAS}\.|heartbeats$)"; then
    log_ok "目標端已出現複寫 topic"
    break
  fi
  if ! kill -0 "$(cat "${MM2_PID_FILE}")" 2>/dev/null; then
    log_error "MM2 行程已結束，最後 30 行 log："
    tail -30 "${MM2_LOG}" >&2
    exit 1
  fi
  sleep 5
  WAITED=$(( WAITED + 5 ))
done

if (( WAITED >= 120 )); then
  log_warn "120 秒內未看到複寫 topic。可能來源端沒有符合 --topics 的 topic，或設定有誤。"
  tail -20 "${MM2_LOG}" >&2
fi

section "完成"
cat >&2 <<EOF
  設定檔   : ${MM2_CONF}
  pid      : $(cat "${MM2_PID_FILE}" 2>/dev/null || echo n/a)
  log      : ${MM2_LOG}
  停止     : kill \$(cat ${MM2_PID_FILE})

  檢查複寫狀態：
    ./scripts/dr/dr-status.sh --source-alias ${SOURCE_ALIAS} \\
        --source ${SOURCE_BOOTSTRAP} --target ${TARGET_BOOTSTRAP}
EOF
