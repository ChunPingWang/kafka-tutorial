#!/usr/bin/env bash
# =============================================================================
# common.sh - 所有腳本共用的函式庫（logging / 前置檢查 / Kafka CLI 包裝）
#
# 使用方式：
#   source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
#
# 這個檔案「只定義函式與變數」，不會自己執行任何動作，可以安全地被 source。
# =============================================================================

# 只載入一次
[[ -n "${_KAFKA_COMMON_LOADED:-}" ]] && return 0
_KAFKA_COMMON_LOADED=1

set -o pipefail

# -----------------------------------------------------------------------------
# 路徑推導
# -----------------------------------------------------------------------------
_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${_COMMON_DIR}/../.." && pwd)"
export REPO_ROOT

# -----------------------------------------------------------------------------
# 載入設定檔（可被環境變數覆寫）
#   優先序：環境變數 > conf/kafka-env.sh > 本檔預設值
# -----------------------------------------------------------------------------
KAFKA_ENV_FILE="${KAFKA_ENV_FILE:-${REPO_ROOT}/conf/kafka-env.sh}"
if [[ -f "${KAFKA_ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${KAFKA_ENV_FILE}"
fi

# ---- 版本與安裝路徑 ----------------------------------------------------------
KAFKA_VERSION="${KAFKA_VERSION:-4.1.2}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"

# 預設安裝在使用者家目錄下，不需要 root。要裝到 /opt 請自行覆寫。
KAFKA_BASE_DIR="${KAFKA_BASE_DIR:-${HOME}/kafka}"
KAFKA_HOME="${KAFKA_HOME:-${KAFKA_BASE_DIR}/current}"
KAFKA_DATA_DIR="${KAFKA_DATA_DIR:-${KAFKA_BASE_DIR}/data}"
KAFKA_LOG_DIR="${KAFKA_LOG_DIR:-${KAFKA_BASE_DIR}/logs}"
KAFKA_CONF_DIR="${KAFKA_CONF_DIR:-${KAFKA_BASE_DIR}/conf}"
KAFKA_RUN_DIR="${KAFKA_RUN_DIR:-${KAFKA_BASE_DIR}/run}"
KAFKA_BACKUP_DIR="${KAFKA_BACKUP_DIR:-${KAFKA_BASE_DIR}/backups}"

# ---- 連線設定 ----------------------------------------------------------------
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
# 需要 SASL/TLS 時指定，例如 conf/security/client.properties
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-}"

# ---- 下載來源 ----------------------------------------------------------------
KAFKA_MIRROR="${KAFKA_MIRROR:-https://downloads.apache.org/kafka}"
KAFKA_ARCHIVE_MIRROR="${KAFKA_ARCHIVE_MIRROR:-https://archive.apache.org/dist/kafka}"

# ---- JVM ---------------------------------------------------------------------
KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx2G -Xms2G}"

# ---- 行為開關 ----------------------------------------------------------------
DRY_RUN="${DRY_RUN:-false}"      # true 時只印出指令不執行
LOG_LEVEL="${LOG_LEVEL:-info}"   # debug | info | warn | error
NO_COLOR="${NO_COLOR:-false}"

# -----------------------------------------------------------------------------
# 顏色與 logging
# -----------------------------------------------------------------------------
if [[ "${NO_COLOR}" == "true" || ! -t 2 ]]; then
  C_RESET="" ; C_RED="" ; C_GRN="" ; C_YEL="" ; C_BLU="" ; C_DIM="" ; C_BOLD=""
else
  C_RESET=$'\033[0m' ; C_RED=$'\033[31m' ; C_GRN=$'\033[32m'
  C_YEL=$'\033[33m'  ; C_BLU=$'\033[34m' ; C_DIM=$'\033[2m' ; C_BOLD=$'\033[1m'
fi

_log_ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }

_should_log() {
  local lvl="$1"
  local -A rank=([debug]=10 [info]=20 [warn]=30 [error]=40)
  (( ${rank[$lvl]:-20} >= ${rank[${LOG_LEVEL}]:-20} ))
}

log_debug() { _should_log debug && printf '%s %s[DEBUG]%s %s\n' "$(_log_ts)" "${C_DIM}"  "${C_RESET}" "$*" >&2; return 0; }
log_info()  { _should_log info  && printf '%s %s[INFO ]%s %s\n' "$(_log_ts)" "${C_BLU}"  "${C_RESET}" "$*" >&2; return 0; }
log_warn()  { _should_log warn  && printf '%s %s[WARN ]%s %s\n' "$(_log_ts)" "${C_YEL}"  "${C_RESET}" "$*" >&2; return 0; }
log_error() { _should_log error && printf '%s %s[ERROR]%s %s\n' "$(_log_ts)" "${C_RED}"  "${C_RESET}" "$*" >&2; return 0; }
log_ok()    { _should_log info  && printf '%s %s[ OK  ]%s %s\n' "$(_log_ts)" "${C_GRN}"  "${C_RESET}" "$*" >&2; return 0; }

die() { log_error "$*"; exit 1; }

# 大標題，讓長腳本的輸出容易掃讀
section() {
  printf '\n%s==> %s%s\n' "${C_BOLD}" "$*" "${C_RESET}" >&2
}

# -----------------------------------------------------------------------------
# 執行包裝：支援 DRY_RUN
# -----------------------------------------------------------------------------
run() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    printf '%s[DRY-RUN]%s %s\n' "${C_YEL}" "${C_RESET}" "$*" >&2
    return 0
  fi
  log_debug "exec: $*"
  "$@"
}

# 需要 root 才能做的事；非 root 時自動加 sudo
run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    run "$@"
  elif command -v sudo >/dev/null 2>&1; then
    run sudo "$@"
  else
    die "需要 root 權限執行：$* （請安裝 sudo 或改用 root 執行）"
  fi
}

# -----------------------------------------------------------------------------
# 通用工具
# -----------------------------------------------------------------------------
need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "找不到必要指令：$1"
}

# retry <次數> <初始秒數> -- <指令...>
# 指數退避重試，用於下載或等待服務啟動
retry() {
  local attempts="$1"; shift
  local delay="$1"; shift
  [[ "${1:-}" == "--" ]] && shift
  local n=1
  until "$@"; do
    if (( n >= attempts )); then
      log_error "重試 ${attempts} 次後仍失敗：$*"
      return 1
    fi
    log_warn "第 ${n} 次失敗，${delay}s 後重試：$*"
    sleep "${delay}"
    delay=$(( delay * 2 ))
    n=$(( n + 1 ))
  done
  return 0
}

# 等待 TCP port 可連線：wait_for_port <host> <port> [timeout_sec]
wait_for_port() {
  local host="$1" port="$2" timeout="${3:-60}"
  local waited=0
  while (( waited < timeout )); do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      return 0
    fi
    sleep 1
    waited=$(( waited + 1 ))
  done
  return 1
}

# 檢查 port 是否已被佔用（安裝前檢查用）
port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :${port}" 2>/dev/null | grep -q ":${port}"
  elif command -v netstat >/dev/null 2>&1; then
    netstat -ltn 2>/dev/null | grep -qE "[:.]${port}[[:space:]]"
  else
    (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null && { exec 3>&- ; return 0; }
    return 1
  fi
}

# -----------------------------------------------------------------------------
# Kafka CLI 包裝
# -----------------------------------------------------------------------------
kafka_bin() {
  local name="$1"
  local p="${KAFKA_HOME}/bin/${name}"
  [[ -x "${p}" ]] || die "找不到 ${p}，請先執行 scripts/install/install-kafka.sh"
  printf '%s' "${p}"
}

# 帶上 --command-config（若有設定安全性設定檔）
_cmd_config_args() {
  if [[ -n "${KAFKA_CLIENT_CONFIG}" && -f "${KAFKA_CLIENT_CONFIG}" ]]; then
    printf -- '--command-config %s' "${KAFKA_CLIENT_CONFIG}"
  fi
}

# kafka_topics --list ... （自動帶入 bootstrap 與安全性設定）
kafka_topics()    { run "$(kafka_bin kafka-topics.sh)"           --bootstrap-server "${BOOTSTRAP_SERVERS}" $(_cmd_config_args) "$@"; }
kafka_configs()   { run "$(kafka_bin kafka-configs.sh)"          --bootstrap-server "${BOOTSTRAP_SERVERS}" $(_cmd_config_args) "$@"; }
kafka_groups()    { run "$(kafka_bin kafka-consumer-groups.sh)"  --bootstrap-server "${BOOTSTRAP_SERVERS}" $(_cmd_config_args) "$@"; }
kafka_acls()      { run "$(kafka_bin kafka-acls.sh)"             --bootstrap-server "${BOOTSTRAP_SERVERS}" $(_cmd_config_args) "$@"; }
# kafka-cluster.sh 要求「子指令在前」，因此單獨包裝
kafka_cluster()   { local sub="$1"; shift; run "$(kafka_bin kafka-cluster.sh)" "${sub}" --bootstrap-server "${BOOTSTRAP_SERVERS}" $(_cmd_config_args) "$@"; }
kafka_metadata()  { run "$(kafka_bin kafka-metadata-quorum.sh)"  --bootstrap-server "${BOOTSTRAP_SERVERS}" $(_cmd_config_args) "$@"; }
kafka_logdirs()   { run "$(kafka_bin kafka-log-dirs.sh)"         --bootstrap-server "${BOOTSTRAP_SERVERS}" $(_cmd_config_args) "$@"; }

# broker 是否已經可以服務（用 API 而非只看 port）
cluster_ready() {
  "$(kafka_bin kafka-broker-api-versions.sh)" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} \
      >/dev/null 2>&1
}

wait_for_cluster() {
  local timeout="${1:-90}" waited=0
  log_info "等待叢集就緒（${BOOTSTRAP_SERVERS}，逾時 ${timeout}s）..."
  while (( waited < timeout )); do
    if cluster_ready; then
      log_ok "叢集已就緒"
      return 0
    fi
    sleep 2
    waited=$(( waited + 2 ))
  done
  log_error "等待叢集逾時"
  return 1
}

# -----------------------------------------------------------------------------
# 產生時間戳（備份命名用）
# -----------------------------------------------------------------------------
timestamp() { date -u '+%Y%m%dT%H%M%SZ'; }

# 人類可讀的位元組
human_bytes() {
  local b="${1:-0}"
  awk -v b="$b" 'BEGIN{
    split("B KB MB GB TB PB", u, " ");
    i=1; while (b >= 1024 && i < 6) { b/=1024; i++ }
    printf (i==1 ? "%d %s\n" : "%.2f %s\n"), b, u[i]
  }'
}

# 確認提示（CI 中用 ASSUME_YES=true 略過）
confirm() {
  local prompt="${1:-確定要繼續嗎？}"
  if [[ "${ASSUME_YES:-false}" == "true" ]]; then
    log_warn "ASSUME_YES=true，自動確認：${prompt}"
    return 0
  fi
  read -r -p "${prompt} [y/N] " reply
  [[ "${reply}" =~ ^[Yy]$ ]]
}
