#!/usr/bin/env bash
# =============================================================================
# install-kafka.sh - 下載、校驗、安裝並設定 Apache Kafka（KRaft 模式）
#
# 用法：
#   ./scripts/install/install-kafka.sh [選項]
#
# 選項：
#   --mode single|cluster   single = 單機 broker+controller（預設）
#                           cluster = 多節點，需搭配 --node-id / --voters
#   --node-id N             本節點的 node.id（cluster 模式必填）
#   --voters LIST           controller quorum，例：1@k1:9093,2@k2:9093,3@k3:9093
#   --roles ROLES           process.roles，預設 broker,controller
#   --advertised-host HOST  對外公告的主機名稱／IP（預設 localhost）
#   --broker-port N         broker listener port（預設 9092）
#   --controller-port N     controller listener port（預設 9093）
#   --jmx-port N            JMX port（預設 9999；同機多 broker 必須錯開）
#   --version X.Y.Z         Kafka 版本（預設 ${KAFKA_VERSION}）
#   --cluster-id ID         既有叢集的 cluster id（加入既有叢集時必填）
#   --skip-verify           跳過 SHA512 校驗（不建議）
#   --force-format          即使資料目錄已存在也重新格式化（會刪資料！）
#   --no-start              安裝與設定完成後不啟動
#   -h, --help              顯示說明
#
# 範例：
#   # 單機學習環境
#   ./scripts/install/install-kafka.sh
#
#   # 三節點叢集的第 2 台
#   ./scripts/install/install-kafka.sh --mode cluster --node-id 2 \
#       --voters 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093 \
#       --advertised-host kafka-2.internal --cluster-id MkU3OEVBNTcwNTJENDM2Qk
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

MODE="single"
NODE_ID="${NODE_ID:-1}"
VOTERS="${CONTROLLER_QUORUM_VOTERS:-}"
ROLES="broker,controller"
ADV_HOST="${ADVERTISED_HOST:-localhost}"
CLUSTER_ID="${CLUSTER_ID:-}"
SKIP_VERIFY=false
FORCE_FORMAT=false
DO_START=true

usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)             MODE="$2"; shift 2 ;;
    --node-id)          NODE_ID="$2"; shift 2 ;;
    --voters)           VOTERS="$2"; shift 2 ;;
    --roles)            ROLES="$2"; shift 2 ;;
    --advertised-host)  ADV_HOST="$2"; shift 2 ;;
    --broker-port)      BROKER_PORT="$2"; shift 2 ;;
    --controller-port)  CONTROLLER_PORT="$2"; shift 2 ;;
    --jmx-port)         JMX_PORT="$2"; shift 2 ;;
    --version)          KAFKA_VERSION="$2"; KAFKA_DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"; shift 2 ;;
    --cluster-id)       CLUSTER_ID="$2"; shift 2 ;;
    --skip-verify)      SKIP_VERIFY=true; shift ;;
    --force-format)     FORCE_FORMAT=true; shift ;;
    --no-start)         DO_START=false; shift ;;
    -h|--help)          usage ;;
    *) die "未知選項：$1（--help 看說明）" ;;
  esac
done

# -----------------------------------------------------------------------------
# 依模式決定參數
# -----------------------------------------------------------------------------
BROKER_PORT="${BROKER_PORT:-9092}"
CONTROLLER_PORT="${CONTROLLER_PORT:-9093}"
# JMX 供監控（Prometheus JMX exporter / jconsole）使用；
# 同一台機器要跑多個 broker 時務必錯開。
JMX_PORT="${JMX_PORT:-9999}"

case "${MODE}" in
  single)
    VOTERS="${VOTERS:-${NODE_ID}@localhost:${CONTROLLER_PORT}}"
    INTERNAL_RF=1 ; INTERNAL_MIN_ISR=1 ; DEFAULT_RF=1 ; MIN_ISR=1
    NUM_PARTITIONS="${NUM_PARTITIONS:-1}"
    AUTO_CREATE_TOPICS="${AUTO_CREATE_TOPICS:-true}"
    GROUP_INITIAL_REBALANCE_DELAY=0
    ;;
  cluster)
    [[ -n "${VOTERS}" ]] || die "cluster 模式必須提供 --voters"
    INTERNAL_RF="${INTERNAL_RF:-3}" ; INTERNAL_MIN_ISR="${INTERNAL_MIN_ISR:-2}"
    DEFAULT_RF="${DEFAULT_RF:-3}"   ; MIN_ISR="${MIN_ISR:-2}"
    NUM_PARTITIONS="${NUM_PARTITIONS:-6}"
    AUTO_CREATE_TOPICS="${AUTO_CREATE_TOPICS:-false}"
    GROUP_INITIAL_REBALANCE_DELAY=3000
    ;;
  *) die "--mode 只能是 single 或 cluster" ;;
esac

# 依機器規格推算執行緒數
CPU_CORES="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
NUM_NETWORK_THREADS="${NUM_NETWORK_THREADS:-$(( CPU_CORES > 3 ? CPU_CORES : 3 ))}"
NUM_IO_THREADS="${NUM_IO_THREADS:-$(( CPU_CORES * 2 > 8 ? CPU_CORES * 2 : 8 ))}"
RETENTION_HOURS="${RETENTION_HOURS:-168}"

# listener 組合（未啟用安全性時的預設；TLS/SASL 請見 README「安全性」章節）
LISTENERS="PLAINTEXT://:${BROKER_PORT},CONTROLLER://:${CONTROLLER_PORT}"
ADVERTISED_LISTENERS="PLAINTEXT://${ADV_HOST}:${BROKER_PORT}"
LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
INTER_BROKER_LISTENER_NAME="PLAINTEXT"

# controller-only 節點不需要公告 broker listener
if [[ "${ROLES}" == "controller" ]]; then
  LISTENERS="CONTROLLER://:${CONTROLLER_PORT}"
  ADVERTISED_LISTENERS=""
fi

section "安裝參數"
cat >&2 <<EOF
  Kafka 版本        : ${KAFKA_VERSION} (Scala ${SCALA_VERSION})
  模式              : ${MODE}
  process.roles     : ${ROLES}
  node.id           : ${NODE_ID}
  quorum voters     : ${VOTERS}
  advertised host   : ${ADV_HOST}
  安裝目錄          : ${KAFKA_BASE_DIR}
  資料目錄          : ${KAFKA_DATA_DIR}
  broker/controller : ${BROKER_PORT} / ${CONTROLLER_PORT}   JMX: ${JMX_PORT}
  設定檔            : ${KAFKA_CONF_DIR}/server.properties
EOF

# -----------------------------------------------------------------------------
# 1. 下載
# -----------------------------------------------------------------------------
section "1/6 下載 Kafka ${KAFKA_VERSION}"
DOWNLOAD_DIR="${KAFKA_BASE_DIR}/downloads"
run mkdir -p "${DOWNLOAD_DIR}" "${KAFKA_BASE_DIR}" "${KAFKA_CONF_DIR}" "${KAFKA_LOG_DIR}" "${KAFKA_DATA_DIR}"

TARBALL="${DOWNLOAD_DIR}/${KAFKA_DIST}.tgz"
if [[ -s "${TARBALL}" ]]; then
  log_info "已存在 ${TARBALL}，略過下載"
else
  URL="${KAFKA_MIRROR}/${KAFKA_VERSION}/${KAFKA_DIST}.tgz"
  ALT_URL="${KAFKA_ARCHIVE_MIRROR}/${KAFKA_VERSION}/${KAFKA_DIST}.tgz"
  log_info "下載 ${URL}"
  if ! retry 3 2 -- curl -fsSL --connect-timeout 20 -o "${TARBALL}.part" "${URL}"; then
    log_warn "主鏡像失敗，改用封存站 ${ALT_URL}"
    retry 3 2 -- curl -fsSL --connect-timeout 20 -o "${TARBALL}.part" "${ALT_URL}" \
      || die "下載失敗。請確認版本 ${KAFKA_VERSION} 存在，或設定 KAFKA_MIRROR"
    URL="${ALT_URL}"
  fi
  run mv "${TARBALL}.part" "${TARBALL}"
  log_ok "下載完成：$(human_bytes "$(stat -c%s "${TARBALL}" 2>/dev/null || stat -f%z "${TARBALL}")")"

  # -- 校驗 --
  if [[ "${SKIP_VERIFY}" == "true" ]]; then
    log_warn "已跳過 SHA512 校驗（--skip-verify）"
  elif command -v sha512sum >/dev/null 2>&1; then
    log_info "下載 SHA512 校驗碼"
    if curl -fsSL --connect-timeout 20 -o "${TARBALL}.sha512" "${URL}.sha512"; then
      EXPECTED="$(tr -d ' \n' < "${TARBALL}.sha512" | sed 's/.*://' | tr 'A-Z' 'a-z')"
      ACTUAL="$(sha512sum "${TARBALL}" | awk '{print $1}')"
      if [[ "${EXPECTED}" == *"${ACTUAL}"* || "${ACTUAL}" == "${EXPECTED}" ]]; then
        log_ok "SHA512 校驗通過"
      else
        rm -f "${TARBALL}"
        die "SHA512 校驗失敗！檔案已刪除。expected=${EXPECTED} actual=${ACTUAL}"
      fi
    else
      log_warn "取不到校驗碼檔，略過校驗"
    fi
  else
    log_warn "沒有 sha512sum，略過校驗"
  fi
fi

# -----------------------------------------------------------------------------
# 2. 解壓縮
# -----------------------------------------------------------------------------
section "2/6 解壓縮"
TARGET="${KAFKA_BASE_DIR}/${KAFKA_DIST}"
if [[ -d "${TARGET}" ]]; then
  log_info "${TARGET} 已存在，略過解壓縮"
else
  run tar -xzf "${TARBALL}" -C "${KAFKA_BASE_DIR}"
  log_ok "解壓縮到 ${TARGET}"
fi

# current 指向目前版本：升級時只要改這個 symlink 就能快速切換／回退
run ln -sfn "${TARGET}" "${KAFKA_HOME}"
log_ok "${KAFKA_HOME} -> ${TARGET}"

# -----------------------------------------------------------------------------
# 3. 產生設定檔
# -----------------------------------------------------------------------------
section "3/6 產生 server.properties"
TMPL="${REPO_ROOT}/conf/templates/server.properties.tmpl"
[[ -f "${TMPL}" ]] || die "找不到範本 ${TMPL}"
SERVER_PROPS="${KAFKA_CONF_DIR}/server.properties"

if [[ -f "${SERVER_PROPS}" ]]; then
  BAK="${SERVER_PROPS}.$(timestamp).bak"
  run cp "${SERVER_PROPS}" "${BAK}"
  log_info "已備份既有設定到 ${BAK}"
fi

render_config() {
  sed \
    -e "s|@@PROCESS_ROLES@@|${ROLES}|g" \
    -e "s|@@NODE_ID@@|${NODE_ID}|g" \
    -e "s|@@CONTROLLER_QUORUM_VOTERS@@|${VOTERS}|g" \
    -e "s|@@LISTENERS@@|${LISTENERS}|g" \
    -e "s|@@ADVERTISED_LISTENERS@@|${ADVERTISED_LISTENERS}|g" \
    -e "s|@@LISTENER_SECURITY_PROTOCOL_MAP@@|${LISTENER_SECURITY_PROTOCOL_MAP}|g" \
    -e "s|@@INTER_BROKER_LISTENER_NAME@@|${INTER_BROKER_LISTENER_NAME}|g" \
    -e "s|@@NUM_NETWORK_THREADS@@|${NUM_NETWORK_THREADS}|g" \
    -e "s|@@NUM_IO_THREADS@@|${NUM_IO_THREADS}|g" \
    -e "s|@@LOG_DIRS@@|${KAFKA_DATA_DIR}|g" \
    -e "s|@@NUM_PARTITIONS@@|${NUM_PARTITIONS}|g" \
    -e "s|@@INTERNAL_RF@@|${INTERNAL_RF}|g" \
    -e "s|@@INTERNAL_MIN_ISR@@|${INTERNAL_MIN_ISR}|g" \
    -e "s|@@DEFAULT_RF@@|${DEFAULT_RF}|g" \
    -e "s|@@MIN_ISR@@|${MIN_ISR}|g" \
    -e "s|@@AUTO_CREATE_TOPICS@@|${AUTO_CREATE_TOPICS}|g" \
    -e "s|@@RETENTION_HOURS@@|${RETENTION_HOURS}|g" \
    -e "s|@@GROUP_INITIAL_REBALANCE_DELAY@@|${GROUP_INITIAL_REBALANCE_DELAY}|g" \
    "${TMPL}"
}

if [[ "${DRY_RUN}" == "true" ]]; then
  log_info "[DRY-RUN] 將產生 ${SERVER_PROPS}"
else
  render_config > "${SERVER_PROPS}"
  # controller-only 節點不能有空的 advertised.listeners
  [[ -z "${ADVERTISED_LISTENERS}" ]] && sed -i.tmp '/^advertised.listeners=$/d' "${SERVER_PROPS}" && rm -f "${SERVER_PROPS}.tmp"
  log_ok "已產生 ${SERVER_PROPS}"
fi

# -----------------------------------------------------------------------------
# 4. 格式化儲存目錄（KRaft 必要步驟）
# -----------------------------------------------------------------------------
section "4/6 格式化儲存目錄"
META_FILE="${KAFKA_DATA_DIR}/meta.properties"

if [[ -f "${META_FILE}" && "${FORCE_FORMAT}" != "true" ]]; then
  EXISTING_ID="$(grep -E '^cluster.id=' "${META_FILE}" | cut -d= -f2- || true)"
  log_info "資料目錄已格式化（cluster.id=${EXISTING_ID}），略過"
  CLUSTER_ID="${EXISTING_ID}"
else
  if [[ -f "${META_FILE}" && "${FORCE_FORMAT}" == "true" ]]; then
    log_warn "--force-format：將清空 ${KAFKA_DATA_DIR} 的所有資料"
    confirm "確定要刪除 ${KAFKA_DATA_DIR} 底下所有資料嗎？" || die "已取消"
    run rm -rf "${KAFKA_DATA_DIR:?}/"*
  fi
  if [[ -z "${CLUSTER_ID}" ]]; then
    CLUSTER_ID="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
    log_info "產生新的 cluster.id：${CLUSTER_ID}"
    log_warn "叢集其他節點必須使用「同一個」cluster.id，請記下來"
  else
    log_info "使用指定的 cluster.id：${CLUSTER_ID}"
  fi
  run "${KAFKA_HOME}/bin/kafka-storage.sh" format \
      --cluster-id "${CLUSTER_ID}" \
      --config "${SERVER_PROPS}" \
      --ignore-formatted
  log_ok "格式化完成"
fi

# 保存叢集資訊，供其他腳本與後續節點使用
if [[ "${DRY_RUN}" != "true" ]]; then
  cat > "${KAFKA_CONF_DIR}/cluster-info" <<EOF
CLUSTER_ID=${CLUSTER_ID}
KAFKA_VERSION=${KAFKA_VERSION}
NODE_ID=${NODE_ID}
PROCESS_ROLES=${ROLES}
CONTROLLER_QUORUM_VOTERS=${VOTERS}
INSTALLED_AT=$(timestamp)
EOF
fi

# -----------------------------------------------------------------------------
# 5. 產生啟停腳本
# -----------------------------------------------------------------------------
section "5/6 產生啟停腳本"
if [[ "${DRY_RUN}" != "true" ]]; then
  cat > "${KAFKA_BASE_DIR}/start.sh" <<EOF
#!/usr/bin/env bash
# 由 install-kafka.sh 產生
set -Eeuo pipefail
export KAFKA_HEAP_OPTS="\${KAFKA_HEAP_OPTS:-${KAFKA_HEAP_OPTS}}"
export LOG_DIR="${KAFKA_LOG_DIR}"
# JMX：監控與 kafka-run-class 工具會用到
export JMX_PORT="\${JMX_PORT:-${JMX_PORT}}"
exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "\$@" "${SERVER_PROPS}"
EOF
  # 用 quoted heredoc 寫出樣板（內容完全不做展開），再把三個路徑替換進去。
  # 這樣可以避免多層跳脫符號寫錯。
  cat > "${KAFKA_BASE_DIR}/stop.sh" <<'STOP_TMPL'
#!/usr/bin/env bash
# 由 install-kafka.sh 產生。務必用 graceful shutdown，
# 讓 broker 把 leader 交接出去並把 log 索引寫完整。
#
# 注意：Kafka 內建的 kafka-server-stop.sh 是用 pattern 比對殺掉「這台機器上所有」
# 的 kafka.Kafka 行程。同一台跑多個 broker（例如本機練習叢集）時會誤殺，
# 所以這裡以安裝時記錄的 pid 為主，必要時再用設定檔路徑精準反查。
set -Eeuo pipefail
PID_FILE="__PID_FILE__"
SERVER_PROPS="__SERVER_PROPS__"
KAFKA_HOME="__KAFKA_HOME__"
SIGNAL="${SIGNAL:-TERM}"

resolve_pid() {
  # 1) pid 檔：必須存在、非空，而且行程還活著
  if [[ -s "${PID_FILE}" ]]; then
    local p
    p="$(tr -d '[:space:]' < "${PID_FILE}")"
    if [[ -n "${p}" ]] && kill -0 "${p}" 2>/dev/null; then
      printf '%s' "${p}"; return 0
    fi
  fi
  # 2) 用「這個 broker 的設定檔路徑」反查，不會誤中同機其他 broker。
  #    樣式開頭綁定 java 執行檔，避免比對到「命令列剛好含有這串字」的 shell。
  local found
  found="$(pgrep -f "^[^ ]*java .*kafka[.]Kafka .*${SERVER_PROPS}( |$)" 2>/dev/null | head -1 || true)"
  if [[ -n "${found}" ]]; then printf '%s' "${found}"; return 0; fi
  return 1
}

if PID="$(resolve_pid)"; then
  echo "送出 SIG${SIGNAL} 給 pid ${PID}"
  kill "-${SIGNAL}" "${PID}"
  for _ in $(seq 1 120); do
    if ! kill -0 "${PID}" 2>/dev/null; then
      rm -f "${PID_FILE}"; echo "已停止"; exit 0
    fi
    sleep 1
  done
  echo "120 秒後仍未結束，請人工檢查 pid ${PID}" >&2
  exit 1
fi

rm -f "${PID_FILE}"
echo "找不到這個 broker 的行程（設定檔：${SERVER_PROPS}），可能已經停止。" >&2
if [[ "${ALLOW_PATTERN_KILL:-false}" == "true" ]]; then
  echo "ALLOW_PATTERN_KILL=true：改用 kafka-server-stop.sh（會殺掉本機所有 broker）" >&2
  exec "${KAFKA_HOME}/bin/kafka-server-stop.sh" "$@"
fi
exit 0
STOP_TMPL

  sed -i \
    -e "s|__PID_FILE__|${KAFKA_BASE_DIR}/kafka.pid|" \
    -e "s|__SERVER_PROPS__|${SERVER_PROPS}|" \
    -e "s|__KAFKA_HOME__|${KAFKA_HOME}|" \
    "${KAFKA_BASE_DIR}/stop.sh"

  chmod +x "${KAFKA_BASE_DIR}/start.sh" "${KAFKA_BASE_DIR}/stop.sh"
  log_ok "${KAFKA_BASE_DIR}/start.sh 、 stop.sh"
fi

# -----------------------------------------------------------------------------
# 6. 啟動
# -----------------------------------------------------------------------------
section "6/6 啟動"
if [[ "${DO_START}" != "true" ]]; then
  log_info "--no-start：略過啟動"
elif [[ "${DRY_RUN}" == "true" ]]; then
  log_info "[DRY-RUN] 略過啟動"
else
  if cluster_ready; then
    log_info "偵測到 ${BOOTSTRAP_SERVERS} 已有服務在跑，略過啟動"
  else
    mkdir -p "${KAFKA_LOG_DIR}"
    nohup "${KAFKA_BASE_DIR}/start.sh" > "${KAFKA_LOG_DIR}/kafka-stdout.log" 2>&1 &
    echo $! > "${KAFKA_BASE_DIR}/kafka.pid"
    log_info "已在背景啟動（pid $(cat "${KAFKA_BASE_DIR}/kafka.pid")），log：${KAFKA_LOG_DIR}/kafka-stdout.log"
    if [[ "${ROLES}" != "controller" ]]; then
      if wait_for_cluster 90; then
        "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" 2>/dev/null \
          | head -1 | sed 's/^/  /' >&2 || true
      else
        log_error "啟動逾時，請檢查 ${KAFKA_LOG_DIR}/kafka-stdout.log 與 ${KAFKA_LOG_DIR}/server.log"
        tail -30 "${KAFKA_LOG_DIR}/kafka-stdout.log" >&2 || true
        exit 1
      fi
    fi
  fi
fi

section "完成"
cat >&2 <<EOF
  cluster.id : ${CLUSTER_ID}
  設定檔     : ${SERVER_PROPS}
  啟動       : ${KAFKA_BASE_DIR}/start.sh
  停止       : ${KAFKA_BASE_DIR}/stop.sh
  下一步     : ./scripts/test/smoke-test.sh
EOF
