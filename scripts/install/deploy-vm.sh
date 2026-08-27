#!/usr/bin/env bash
# =============================================================================
# deploy-vm.sh - 在一台 VM（或實體機）上完成「正式環境等級」的單節點佈署
#
# 與 install-kafka.sh 的差別：
#   install-kafka.sh 只負責「裝起來、跑起來」（學習環境，nohup 啟動）。
#   本腳本做的是 README 附錄 F 的完整 runbook：
#     1. 前置檢查（preflight）
#     2. 建立專用服務帳號與目錄（不用 root 跑 broker）
#     3. 以服務帳號執行 install-kafka.sh --no-start
#     4. 套用 OS 調校（sysctl / limits，可用 --skip-tuning 跳過）
#     5. 渲染並安裝 systemd unit（graceful shutdown、資源限制、OOM 保護）
#     6. systemctl enable --now 啟動
#     7. 驗證：health-check + smoke-test
#
# 用法：
#   ./scripts/install/deploy-vm.sh                        # 全預設：/opt/kafka
#   ./scripts/install/deploy-vm.sh --base-dir /data/kafka --heap "-Xmx6G -Xms6G"
#   ./scripts/install/deploy-vm.sh --controller-port 9094 # 9093 被別人用時
#   DRY_RUN=true ./scripts/install/deploy-vm.sh           # 只看會做什麼
#
# 選項：
#   --base-dir DIR         安裝根目錄（預設 /opt/kafka；資料在 DIR/data）
#   --user NAME            服務帳號（預設 kafka，不存在會建立）
#   --heap OPTS            JVM heap（預設 -Xmx1G -Xms1G；正式環境建議 6G）
#   --advertised-host H    對外公告位址（預設本機 hostname）
#   --broker-port N        （預設 9092）
#   --controller-port N    （預設 9093）
#   --cluster-id ID        加入既有叢集時指定
#   --tarball FILE         已下載好的 kafka tgz（跳過下載）
#   --java-home DIR        服務帳號要用的 JDK（預設自動偵測；
#                          JDK 裝在家目錄時必須指定一個系統路徑的 JDK）
#   --skip-tuning          不動 sysctl / limits
#   --skip-verify          佈署完不跑驗證
#   -h, --help             顯示說明
#
# 需要：可以 sudo 的帳號、systemd、網路（或 --tarball）。
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

BASE_DIR="/opt/kafka"
SERVICE_USER="kafka"
HEAP="-Xmx1G -Xms1G"
ADV_HOST="$(hostname -f 2>/dev/null || hostname 2>/dev/null || echo "${HOSTNAME:-localhost}")"
BROKER_PORT=9092
CONTROLLER_PORT=9093
CLUSTER_ID_ARG=""
TARBALL=""
JAVA_HOME_DIR="${JAVA_HOME_DIR:-}"
SKIP_TUNING=false
SKIP_VERIFY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-dir)        BASE_DIR="$2"; shift 2 ;;
    --user)            SERVICE_USER="$2"; shift 2 ;;
    --heap)            HEAP="$2"; shift 2 ;;
    --advertised-host) ADV_HOST="$2"; shift 2 ;;
    --broker-port)     BROKER_PORT="$2"; shift 2 ;;
    --controller-port) CONTROLLER_PORT="$2"; shift 2 ;;
    --cluster-id)      CLUSTER_ID_ARG="$2"; shift 2 ;;
    --tarball)         TARBALL="$2"; shift 2 ;;
    --java-home)       JAVA_HOME_DIR="$2"; shift 2 ;;
    --skip-tuning)     SKIP_TUNING=true; shift ;;
    --skip-verify)     SKIP_VERIFY=true; shift ;;
    -h|--help)         sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1（--help 看說明）" ;;
  esac
done

need_cmd systemctl
command -v systemctl >/dev/null || die "本腳本需要 systemd"

DATA_DIR="${BASE_DIR}/data"
CONF_DIR="${BASE_DIR}/conf"
LOG_DIR="${BASE_DIR}/logs"
UNIT_FILE="/etc/systemd/system/kafka.service"

section "VM 佈署參數"
cat >&2 <<EOF
  安裝根目錄   : ${BASE_DIR}
  服務帳號     : ${SERVICE_USER}
  heap         : ${HEAP}
  advertised   : ${ADV_HOST}
  broker/ctrl  : ${BROKER_PORT} / ${CONTROLLER_PORT}
  OS 調校      : $( [[ "${SKIP_TUNING}" == true ]] && echo "略過" || echo "套用 sysctl + limits" )
EOF

# -----------------------------------------------------------------------------
section "1/7 前置檢查"
# preflight 的警告不擋佈署，失敗（exit 2）才擋
PF_RC=0
"${REPO_ROOT}/scripts/install/preflight.sh" >&2 || PF_RC=$?
if (( PF_RC >= 2 )); then
  die "preflight 有未通過項目，請先處理（或逐項確認後重跑）"
fi

# port 檢查提前做：unit 啟動失敗比這裡難查得多
port_in_use "${BROKER_PORT}"     && die "port ${BROKER_PORT} 已被佔用，請換 --broker-port"
port_in_use "${CONTROLLER_PORT}" && die "port ${CONTROLLER_PORT} 已被佔用，請換 --controller-port"

# -----------------------------------------------------------------------------
section "2/7 服務帳號與目錄"
if id "${SERVICE_USER}" >/dev/null 2>&1; then
  log_info "帳號 ${SERVICE_USER} 已存在"
else
  run_root useradd --system --no-create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
  log_ok "已建立系統帳號 ${SERVICE_USER}"
fi
run_root mkdir -p "${BASE_DIR}" "${DATA_DIR}" "${CONF_DIR}" "${LOG_DIR}" "${BASE_DIR}/downloads"
run_root chown -R "${SERVICE_USER}:${SERVICE_USER}" "${BASE_DIR}"

# -----------------------------------------------------------------------------
section "3/7 安裝 Kafka（以 ${SERVICE_USER} 身分）"
# 有現成 tarball 就播種進去，避免重複下載
if [[ -n "${TARBALL}" ]]; then
  [[ -s "${TARBALL}" ]] || die "找不到 --tarball 指定的檔案：${TARBALL}"
  run_root cp "${TARBALL}" "${BASE_DIR}/downloads/"
  run_root chown "${SERVICE_USER}:${SERVICE_USER}" "${BASE_DIR}/downloads/$(basename "${TARBALL}")"
  log_ok "已使用現成 tarball：$(basename "${TARBALL}")"
fi

INSTALL_ARGS=(--mode single --no-start
              --advertised-host "${ADV_HOST}"
              --broker-port "${BROKER_PORT}"
              --controller-port "${CONTROLLER_PORT}")
[[ -n "${CLUSTER_ID_ARG}" ]] && INSTALL_ARGS+=(--cluster-id "${CLUSTER_ID_ARG}")

# 服務帳號通常讀不到操作者家目錄下的 repo（家目錄多半是 700），
# 因此以 root 執行安裝，完成後把整個安裝目錄交還給服務帳號。
# broker 本身仍以服務帳號執行（見 systemd unit 的 User=）。
# PATH 也要帶過去：sudo 的 secure_path 常常沒有 Java
run_root env \
    HOME="${BASE_DIR}" \
    PATH="${PATH}" \
    KAFKA_BASE_DIR="${BASE_DIR}" \
    BOOTSTRAP_SERVERS="localhost:${BROKER_PORT}" \
    DRY_RUN="${DRY_RUN}" \
    bash "${REPO_ROOT}/scripts/install/install-kafka.sh" "${INSTALL_ARGS[@]}"
run_root chown -R "${SERVICE_USER}:${SERVICE_USER}" "${BASE_DIR}"

# -----------------------------------------------------------------------------
section "4/7 OS 調校"
if [[ "${SKIP_TUNING}" == "true" ]]; then
  log_info "--skip-tuning：略過"
else
  run_root cp "${REPO_ROOT}/conf/templates/sysctl-kafka.conf" /etc/sysctl.d/99-kafka.conf
  # 逐鍵套用：虛擬化環境（WSL2、部分容器化 VM）可能有唯讀的鍵，跳過即可
  APPLIED=0; SKIPPED=0
  while IFS='=' read -r key val; do
    key="$(echo "${key}" | tr -d ' ')"
    [[ -z "${key}" || "${key}" == \#* ]] && continue
    if run_root sysctl -w "${key}=$(echo "${val}" | sed 's/^ *//')" >/dev/null 2>&1; then
      APPLIED=$((APPLIED+1))
    else
      SKIPPED=$((SKIPPED+1)); log_warn "無法套用 ${key}（虛擬化環境限制？）"
    fi
  done < <(grep -E '^[a-z]' /etc/sysctl.d/99-kafka.conf)
  log_ok "sysctl：套用 ${APPLIED} 項，跳過 ${SKIPPED} 項"

  run_root cp "${REPO_ROOT}/conf/templates/limits-kafka.conf" /etc/security/limits.d/99-kafka.conf
  log_ok "limits：/etc/security/limits.d/99-kafka.conf（systemd 啟動時以 unit 的 Limit* 為準）"
fi

# -----------------------------------------------------------------------------
section "5/7 安裝 systemd unit"
TMPL="${REPO_ROOT}/conf/templates/kafka.service.tmpl"
[[ -f "${TMPL}" ]] || die "找不到 ${TMPL}"

if [[ "${DRY_RUN}" == "true" ]]; then
  log_info "[DRY-RUN] 將渲染 ${TMPL} -> ${UNIT_FILE}"
else
  sed -e "s|@@KAFKA_HOME@@|${BASE_DIR}/current|g" \
      -e "s|@@KAFKA_CONF@@|${CONF_DIR}/server.properties|g" \
      -e "s|@@KAFKA_LOG_DIR@@|${LOG_DIR}|g" \
      -e "s|@@KAFKA_DATA_DIR@@|${DATA_DIR}|g" \
      -e "s|@@KAFKA_USER@@|${SERVICE_USER}|g" \
      -e "s|@@HEAP@@|${HEAP}|g" \
      "${TMPL}" | run_root tee "${UNIT_FILE}" >/dev/null
  log_ok "已寫入 ${UNIT_FILE}"
fi

# --- JAVA_HOME：unit 的 ProtectHome=true 會擋掉家目錄下的 JDK（SDKMAN 等）---
if [[ -z "${JAVA_HOME_DIR}" ]]; then
  JAVA_BIN="$(readlink -f "$(command -v java 2>/dev/null)" 2>/dev/null || true)"
  [[ -n "${JAVA_BIN}" ]] && JAVA_HOME_DIR="$(dirname "$(dirname "${JAVA_BIN}")")"
fi
[[ -n "${JAVA_HOME_DIR}" ]] || die "找不到 java，請安裝 JDK 17+ 或用 --java-home 指定"
if [[ "${JAVA_HOME_DIR}" == /home/* || "${JAVA_HOME_DIR}" == /root/* ]]; then
  die "JDK 位於家目錄（${JAVA_HOME_DIR}），服務帳號讀不到（unit 也設了 ProtectHome）。
  請安裝系統 JDK（如 dnf/apt install java-21-openjdk-headless），
  或把 JDK 複製到系統路徑後用 --java-home 指定，例如：
    sudo cp -a ${JAVA_HOME_DIR} /opt/java-21 && ./scripts/install/deploy-vm.sh --java-home /opt/java-21 ..."
fi
if [[ "${DRY_RUN}" != "true" ]]; then
  run_root sudo -u "${SERVICE_USER}" test -x "${JAVA_HOME_DIR}/bin/java" \
    || die "服務帳號 ${SERVICE_USER} 無法執行 ${JAVA_HOME_DIR}/bin/java，請檢查權限"
  run_root mkdir -p "${UNIT_FILE}.d"
  printf '[Service]\nEnvironment="JAVA_HOME=%s"\n' "${JAVA_HOME_DIR}" \
    | run_root tee "${UNIT_FILE}.d/10-java.conf" >/dev/null
  log_ok "JAVA_HOME=${JAVA_HOME_DIR}（drop-in：${UNIT_FILE}.d/10-java.conf）"
fi
run_root systemctl daemon-reload

# -----------------------------------------------------------------------------
section "6/7 啟動"
run_root systemctl enable --now kafka
if [[ "${DRY_RUN}" != "true" ]]; then
  export BOOTSTRAP_SERVERS="localhost:${BROKER_PORT}"
  export KAFKA_BASE_DIR="${BASE_DIR}"
  if ! KAFKA_HOME="${BASE_DIR}/current" wait_for_cluster 90; then
    log_error "broker 未在 90 秒內就緒，最近的 journal："
    run_root journalctl -u kafka -n 30 --no-pager >&2 || true
    exit 1
  fi
  run_root systemctl --no-pager status kafka | sed -n '1,5p' >&2 || true
fi

# -----------------------------------------------------------------------------
section "7/7 驗證"
if [[ "${SKIP_VERIFY}" == "true" || "${DRY_RUN}" == "true" ]]; then
  log_info "略過驗證"
else
  RC=0
  BOOTSTRAP_SERVERS="localhost:${BROKER_PORT}" KAFKA_BASE_DIR="${BASE_DIR}" \
    "${REPO_ROOT}/scripts/ops/health-check.sh" || RC=$?
  (( RC <= 1 )) || die "health-check 回報嚴重異常（exit ${RC}）"
  BOOTSTRAP_SERVERS="localhost:${BROKER_PORT}" KAFKA_BASE_DIR="${BASE_DIR}" \
    "${REPO_ROOT}/scripts/test/smoke-test.sh" || die "smoke-test 未通過"
fi

section "完成"
cat >&2 <<EOF
  服務        : systemctl status kafka
  日誌        : journalctl -u kafka -f
  設定        : ${CONF_DIR}/server.properties
  資料        : ${DATA_DIR}
  連線        : localhost:${BROKER_PORT}
  下一步      : 見 README 附錄 F（監控、備份排程、加入叢集）
EOF
