#!/usr/bin/env bash
# =============================================================================
# preflight.sh - 安裝前環境檢查
#
# 檢查項目：作業系統、Java、記憶體、磁碟、檔案描述符、port、核心參數、時間同步
# 退出碼：0 = 全數通過或僅有警告；1 = 有致命問題
#
# 用法：
#   ./scripts/install/preflight.sh            # 檢查單機安裝
#   STRICT=true ./scripts/install/preflight.sh # 警告也視為失敗（正式環境建議）
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

STRICT="${STRICT:-false}"
FAIL=0
WARN=0

pass() { printf '  %s✔%s %s\n' "${C_GRN}" "${C_RESET}" "$*"; }
warn() { printf '  %s!%s %s\n' "${C_YEL}" "${C_RESET}" "$*"; WARN=$(( WARN + 1 )); }
fail() { printf '  %s✘%s %s\n' "${C_RED}" "${C_RESET}" "$*"; FAIL=$(( FAIL + 1 )); }

section "1. 作業系統"
if [[ "$(uname -s)" == "Linux" ]]; then
  pass "Linux $(uname -r)"
  [[ -f /etc/os-release ]] && pass "發行版：$(. /etc/os-release && echo "${PRETTY_NAME}")"
elif [[ "$(uname -s)" == "Darwin" ]]; then
  warn "macOS：適合本機學習，不建議作為正式環境 broker"
else
  warn "未測試的作業系統：$(uname -s)"
fi

section "2. Java"
if command -v java >/dev/null 2>&1; then
  JAVA_VER_RAW="$(java -version 2>&1 | head -1)"
  JAVA_MAJOR="$(java -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)"
  if [[ -z "${JAVA_MAJOR}" ]]; then
    warn "無法解析 Java 版本：${JAVA_VER_RAW}"
  elif (( JAVA_MAJOR >= 17 )); then
    pass "Java ${JAVA_MAJOR}（Kafka 4.x broker 需要 17+）"
  else
    fail "Java ${JAVA_MAJOR} 太舊；Kafka 4.x broker 需要 Java 17 或以上"
  fi
else
  fail "找不到 java。請安裝 JDK 17+（例：apt install openjdk-21-jdk-headless）"
fi

section "3. 記憶體"
if [[ -r /proc/meminfo ]]; then
  MEM_KB="$(awk '/^MemTotal:/{print $2}' /proc/meminfo)"
  MEM_GB=$(( MEM_KB / 1024 / 1024 ))
  if (( MEM_GB >= 16 )); then
    pass "實體記憶體 ${MEM_GB} GB"
  elif (( MEM_GB >= 4 )); then
    warn "實體記憶體僅 ${MEM_GB} GB：可供學習，正式環境建議 ≥ 32 GB"
  else
    fail "實體記憶體僅 ${MEM_GB} GB，過低"
  fi
else
  warn "無法讀取記憶體資訊（非 Linux？）"
fi

section "4. 磁碟"
DISK_TARGET="$(dirname "${KAFKA_DATA_DIR}")"
mkdir -p "${DISK_TARGET}" 2>/dev/null || true
if [[ -d "${DISK_TARGET}" ]]; then
  AVAIL_KB="$(df -Pk "${DISK_TARGET}" | awk 'NR==2{print $4}')"
  AVAIL_GB=$(( AVAIL_KB / 1024 / 1024 ))
  if (( AVAIL_GB >= 50 )); then
    pass "${DISK_TARGET} 可用空間 ${AVAIL_GB} GB"
  elif (( AVAIL_GB >= 5 )); then
    warn "${DISK_TARGET} 可用空間僅 ${AVAIL_GB} GB：學習夠用，正式環境請規劃專屬磁碟"
  else
    fail "${DISK_TARGET} 可用空間僅 ${AVAIL_GB} GB，不足以啟動"
  fi
  FS_TYPE="$(df -PT "${DISK_TARGET}" 2>/dev/null | awk 'NR==2{print $2}')"
  case "${FS_TYPE}" in
    xfs|ext4) pass "檔案系統 ${FS_TYPE}（建議值）" ;;
    "")       warn "無法判斷檔案系統型別" ;;
    *)        warn "檔案系統 ${FS_TYPE}：Kafka 建議使用 XFS 或 ext4；切勿使用 NFS 存放 log.dirs" ;;
  esac
fi

section "5. 檔案描述符與行程數"
NOFILE="$(ulimit -n)"
if [[ "${NOFILE}" == "unlimited" ]] || (( NOFILE >= 100000 )); then
  pass "ulimit -n = ${NOFILE}"
elif (( NOFILE >= 10000 )); then
  warn "ulimit -n = ${NOFILE}：正式環境建議 ≥ 100000（見 README「作業系統調校」）"
else
  fail "ulimit -n = ${NOFILE} 過低，broker 會出現 Too many open files"
fi
NPROC="$(ulimit -u 2>/dev/null || echo unknown)"
pass "ulimit -u = ${NPROC}"

section "6. 網路 port"
for p in 9092 9093; do
  if port_in_use "${p}"; then
    warn "port ${p} 已被佔用（若是既有的 Kafka 屬正常）"
  else
    pass "port ${p} 未被佔用"
  fi
done

section "7. 核心參數"
check_sysctl() {
  local key="$1" want="$2" cmp="${3:-ge}"
  local cur
  cur="$(sysctl -n "${key}" 2>/dev/null || echo "")"
  if [[ -z "${cur}" ]]; then warn "無法讀取 ${key}"; return; fi
  if [[ "${cmp}" == "ge" ]] && (( cur >= want )); then
    pass "${key} = ${cur}"
  elif [[ "${cmp}" == "le" ]] && (( cur <= want )); then
    pass "${key} = ${cur}"
  else
    warn "${key} = ${cur}（建議 ${cmp} ${want}）"
  fi
}
check_sysctl vm.swappiness 10 le
check_sysctl vm.max_map_count 262144 ge
check_sysctl net.core.somaxconn 1024 ge
check_sysctl fs.file-max 1000000 ge

section "8. 時間同步"
if command -v timedatectl >/dev/null 2>&1; then
  if timedatectl show -p NTPSynchronized --value 2>/dev/null | grep -q yes; then
    pass "NTP 已同步"
  else
    warn "NTP 未同步：時間偏移會影響 log retention 與 consumer lag 判讀"
  fi
elif command -v chronyc >/dev/null 2>&1 || command -v ntpq >/dev/null 2>&1; then
  pass "偵測到時間同步服務"
else
  warn "找不到時間同步服務（chrony / ntp / systemd-timesyncd）"
fi

section "9. 必要工具"
for c in curl tar awk sed grep df; do
  if command -v "$c" >/dev/null 2>&1; then pass "$c"; else fail "缺少 $c"; fi
done
for c in sha512sum gpg jq; do
  if command -v "$c" >/dev/null 2>&1; then pass "$c（選用）"; else warn "缺少 $c（選用，影響校驗/JSON 解析）"; fi
done

section "檢查結果"
printf '  失敗：%d  警告：%d\n' "${FAIL}" "${WARN}"
if (( FAIL > 0 )); then
  log_error "有 ${FAIL} 項致命問題，請先修正再安裝"
  exit 1
fi
if [[ "${STRICT}" == "true" ]] && (( WARN > 0 )); then
  log_error "STRICT=true 且有 ${WARN} 項警告"
  exit 1
fi
log_ok "前置檢查通過"
