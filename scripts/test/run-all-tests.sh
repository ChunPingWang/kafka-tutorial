#!/usr/bin/env bash
# =============================================================================
# run-all-tests.sh - 一次跑完所有測試（安裝驗收 / CI / 上線前檢查）
#
# 執行順序（由淺入深）：
#   1. preflight     環境檢查
#   2. health-check  叢集健康
#   3. smoke-test    核心功能
#   4. perf-test     效能基準（--quick 時跑短版）
#   5. backup + verify  備份與備份驗證
#   6. resilience    故障注入（需 --with-failure-injection）
#
# 用法：
#   ./scripts/test/run-all-tests.sh
#   ./scripts/test/run-all-tests.sh --quick
#   ./scripts/test/run-all-tests.sh --with-failure-injection --docker-broker kafka-2
#
# 退出碼：0 = 全部通過
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

QUICK=false
WITH_FI=false
DOCKER_BROKER=""
SKIP_PERF=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick)                  QUICK=true; shift ;;
    --with-failure-injection) WITH_FI=true; shift ;;
    --docker-broker)          DOCKER_BROKER="$2"; shift 2 ;;
    --skip-perf)              SKIP_PERF=true; shift ;;
    -h|--help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

declare -a NAMES=() STATUSES=() DURATIONS=()
OVERALL=0

run_stage() {
  local name="$1"; shift
  section "▶ ${name}"
  local start rc=0
  start="$(date +%s)"
  if "$@"; then rc=0; else rc=$?; fi
  local dur=$(( $(date +%s) - start ))
  NAMES+=("${name}")
  DURATIONS+=("${dur}")
  if (( rc == 0 )); then
    STATUSES+=("PASS")
    log_ok "${name} 通過（${dur}s）"
  else
    STATUSES+=("FAIL(${rc})")
    OVERALL=1
    log_error "${name} 失敗，退出碼 ${rc}（${dur}s）"
  fi
  return 0
}

TS="$(timestamp)"
REPORT_DIR="${KAFKA_BASE_DIR}/test-reports/${TS}"
mkdir -p "${REPORT_DIR}"

section "測試總覽"
cat >&2 <<EOF
  bootstrap : ${BOOTSTRAP_SERVERS}
  快速模式  : ${QUICK}
  故障注入  : ${WITH_FI}
  報告目錄  : ${REPORT_DIR}
EOF

# 1 ---------------------------------------------------------------------------
run_stage "1. 環境前置檢查" bash -c \
  "'${REPO_ROOT}/scripts/install/preflight.sh' 2>&1 | tee '${REPORT_DIR}/1-preflight.log'"

# 2 ---------------------------------------------------------------------------
# health-check 退出碼 1 代表「有警告」，在測試流程中不算失敗
run_stage "2. 叢集健康檢查" bash -c \
  "'${REPO_ROOT}/scripts/ops/health-check.sh' 2>&1 | tee '${REPORT_DIR}/2-health.log'; rc=\${PIPESTATUS[0]}; [[ \${rc} -le 1 ]]"

# 3 ---------------------------------------------------------------------------
run_stage "3. 冒煙測試" bash -c \
  "'${REPO_ROOT}/scripts/test/smoke-test.sh' 2>&1 | tee '${REPORT_DIR}/3-smoke.log'; exit \${PIPESTATUS[0]}"

# 4 ---------------------------------------------------------------------------
if [[ "${SKIP_PERF}" == "true" ]]; then
  log_info "略過效能測試（--skip-perf）"
else
  PERF_ARGS=""
  [[ "${QUICK}" == "true" ]] && PERF_ARGS="--quick"
  run_stage "4. 效能基準" bash -c \
    "'${REPO_ROOT}/scripts/test/perf-test.sh' ${PERF_ARGS} 2>&1 | tee '${REPORT_DIR}/4-perf.log'; exit \${PIPESTATUS[0]}"
fi

# 5 ---------------------------------------------------------------------------
run_stage "5. 備份" bash -c \
  "'${REPO_ROOT}/scripts/backup/backup-cluster.sh' --output '${REPORT_DIR}/backup' 2>&1 | tee '${REPORT_DIR}/5-backup.log'; exit \${PIPESTATUS[0]}"

run_stage "6. 備份驗證" bash -c \
  "'${REPO_ROOT}/scripts/backup/verify-backup.sh' \$(ls -d '${REPORT_DIR}/backup'/2*Z | tail -1) 2>&1 | tee '${REPORT_DIR}/6-verify.log'; exit \${PIPESTATUS[0]}"

# 6 ---------------------------------------------------------------------------
if [[ "${WITH_FI}" == "true" ]]; then
  FI_ARGS=()
  [[ -n "${DOCKER_BROKER}" ]] && FI_ARGS=(--docker "${DOCKER_BROKER}")
  run_stage "7. 故障注入" bash -c \
    "'${REPO_ROOT}/scripts/test/resilience-test.sh' ${FI_ARGS[*]+${FI_ARGS[*]}} 2>&1 | tee '${REPORT_DIR}/7-resilience.log'; exit \${PIPESTATUS[0]}"
else
  log_info "略過故障注入（加 --with-failure-injection 啟用）"
fi

# -----------------------------------------------------------------------------
section "測試總結"
printf '\n  %-24s %-12s %s\n' "階段" "結果" "耗時"
printf '  %s\n' "$(printf '%.0s-' {1..50})"
for i in "${!NAMES[@]}"; do
  colour="${C_GRN}"
  [[ "${STATUSES[$i]}" == FAIL* ]] && colour="${C_RED}"
  printf '  %-24s %b%-12s%b %ss\n' "${NAMES[$i]}" "${colour}" "${STATUSES[$i]}" "${C_RESET}" "${DURATIONS[$i]}"
done

{
  echo "test_run=${TS}"
  echo "bootstrap=${BOOTSTRAP_SERVERS}"
  for i in "${!NAMES[@]}"; do
    printf '%s|%s|%s\n' "${NAMES[$i]}" "${STATUSES[$i]}" "${DURATIONS[$i]}"
  done
  echo "overall=$( (( OVERALL == 0 )) && echo PASS || echo FAIL )"
} > "${REPORT_DIR}/summary.txt"

printf '\n  報告：%s\n' "${REPORT_DIR}" >&2
if (( OVERALL == 0 )); then
  log_ok "所有測試通過"
else
  log_error "有測試失敗，詳見 ${REPORT_DIR}"
fi
exit "${OVERALL}"
