#!/usr/bin/env bash
# =============================================================================
# rolling-restart.sh - 滾動重啟／滾動升級（零停機維運的核心動作）
#
# 原則：一次只動一台，每台重啟後必須等到「所有 partition 完全同步」
#       才動下一台。否則同時失去兩個副本 = 資料遺失風險。
#
# 用法：
#   # 本機單節點（學習環境）
#   ./scripts/ops/rolling-restart.sh --local
#
#   # 多節點（透過 SSH）
#   ./scripts/ops/rolling-restart.sh --hosts kafka-1,kafka-2,kafka-3
#
#   # 滾動升級到新版本
#   ./scripts/ops/rolling-restart.sh --hosts kafka-1,kafka-2,kafka-3 --upgrade 4.3.1
#
# 選項：
#   --local              重啟本機的 Kafka
#   --hosts LIST         逗號分隔的主機清單（依序處理）
#   --upgrade VERSION    重啟前先切換 ${KAFKA_HOME} symlink 到指定版本
#   --wait-seconds N     每台之間額外等待（預設 30）
#   --max-wait N         等待完全同步的逾時秒數（預設 600）
#   --dry-run            只印出會做什麼
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

LOCAL_MODE=false
HOSTS=""
UPGRADE_VERSION=""
WAIT_SECONDS=30
MAX_WAIT=600
SSH_USER="${SSH_USER:-$(id -un)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local)         LOCAL_MODE=true; shift ;;
    --hosts)         HOSTS="$2"; shift 2 ;;
    --upgrade)       UPGRADE_VERSION="$2"; shift 2 ;;
    --wait-seconds)  WAIT_SECONDS="$2"; shift 2 ;;
    --max-wait)      MAX_WAIT="$2"; shift 2 ;;
    --dry-run)       DRY_RUN=true; shift ;;
    -h|--help)       sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

[[ "${LOCAL_MODE}" == "true" || -n "${HOSTS}" ]] || die "請指定 --local 或 --hosts"

# -----------------------------------------------------------------------------
# 等待叢集完全同步：沒有 under-replicated、沒有 offline partition
# -----------------------------------------------------------------------------
wait_fully_replicated() {
  local waited=0
  log_info "等待所有 partition 完全同步（逾時 ${MAX_WAIT}s）..."
  while (( waited < MAX_WAIT )); do
    if cluster_ready; then
      local ur unav
      ur="$(kafka_topics --describe --under-replicated-partitions 2>/dev/null | grep -c 'Topic:' || true)"
      unav="$(kafka_topics --describe --unavailable-partitions 2>/dev/null | grep -c 'Topic:' || true)"
      if (( ur == 0 )) && (( unav == 0 )); then
        log_ok "完全同步（under-replicated=0, unavailable=0）"
        return 0
      fi
      log_info "  under-replicated=${ur} unavailable=${unav}（已等 ${waited}s）"
    else
      log_info "  叢集尚未可連線（已等 ${waited}s）"
    fi
    sleep 10
    waited=$(( waited + 10 ))
  done
  log_error "等待完全同步逾時；為避免資料風險，停止滾動重啟"
  return 1
}

# -----------------------------------------------------------------------------
# 重啟前的安全前置檢查
# -----------------------------------------------------------------------------
preflight_gate() {
  section "重啟前檢查"
  cluster_ready || die "叢集目前不可連線，先修復再重啟"

  local ur unav umisr
  ur="$(kafka_topics --describe --under-replicated-partitions 2>/dev/null | grep -c 'Topic:' || true)"
  unav="$(kafka_topics --describe --unavailable-partitions 2>/dev/null | grep -c 'Topic:' || true)"
  umisr="$(kafka_topics --describe --under-min-isr-partitions 2>/dev/null | grep -c 'Topic:' || true)"

  printf '  under-replicated : %s\n' "${ur}"
  printf '  unavailable      : %s\n' "${unav}"
  printf '  under-min-isr    : %s\n' "${umisr}"

  if (( ur > 0 || unav > 0 || umisr > 0 )); then
    log_error "叢集尚未健康，此時重啟會擴大故障面。請先讓副本追上。"
    exit 1
  fi

  # RF=1 的 topic 在重啟期間一定會不可用，先警告
  local rf1
  rf1="$(kafka_topics --describe 2>/dev/null | awk '/^Topic: /&&$8==1{print $2}' | tr '\n' ' ')"
  if [[ -n "${rf1// /}" ]]; then
    log_warn "下列 topic 的 RF=1，重啟期間必定短暫不可用：${rf1}"
    confirm "仍要繼續嗎？" || exit 1
  fi
  log_ok "前置檢查通過"
}

# -----------------------------------------------------------------------------
# 單機重啟
# -----------------------------------------------------------------------------
restart_local() {
  section "重啟本機 Kafka"

  if [[ -n "${UPGRADE_VERSION}" ]]; then
    local target="${KAFKA_BASE_DIR}/kafka_${SCALA_VERSION}-${UPGRADE_VERSION}"
    [[ -d "${target}" ]] || die "找不到 ${target}，請先下載該版本（install-kafka.sh --version ${UPGRADE_VERSION} --no-start）"
    log_info "切換 ${KAFKA_HOME} -> ${target}"
    run ln -sfn "${target}" "${KAFKA_HOME}"
  fi

  log_info "送出 graceful shutdown"
  run "${KAFKA_BASE_DIR}/stop.sh" || log_warn "stop.sh 回傳非零（可能本來就沒在跑）"

  # 等待 port 釋放，最多 120 秒
  local waited=0
  while port_in_use 9092 && (( waited < 120 )); do sleep 2; waited=$(( waited + 2 )); done
  if port_in_use 9092; then
    log_error "9092 在 120 秒後仍被佔用，broker 沒有正常關閉"
    log_error "請人工檢查後再繼續（切勿直接 kill -9，會導致 log 復原時間變長）"
    exit 1
  fi
  log_ok "已停止"

  log_info "啟動"
  if [[ "${DRY_RUN}" != "true" ]]; then
    nohup "${KAFKA_BASE_DIR}/start.sh" > "${KAFKA_LOG_DIR}/kafka-stdout.log" 2>&1 &
    echo $! > "${KAFKA_BASE_DIR}/kafka.pid"
  fi
  wait_for_cluster 180 || die "啟動後無法連線"
}

# -----------------------------------------------------------------------------
# 遠端重啟（SSH）
# -----------------------------------------------------------------------------
restart_remote() {
  local host="$1"
  section "重啟 ${host}"

  local upgrade_cmd=""
  if [[ -n "${UPGRADE_VERSION}" ]]; then
    upgrade_cmd="ln -sfn ${KAFKA_BASE_DIR}/kafka_${SCALA_VERSION}-${UPGRADE_VERSION} ${KAFKA_HOME} && "
  fi

  # 優先用 systemd（正式環境建議做法），否則退回 start.sh / stop.sh
  local remote_script
  remote_script="$(cat <<EOF
set -e
${upgrade_cmd}true
if systemctl is-active --quiet kafka 2>/dev/null; then
  sudo systemctl restart kafka
else
  ${KAFKA_BASE_DIR}/stop.sh || true
  for i in \$(seq 1 60); do
    ss -ltn 2>/dev/null | grep -q ':9092' || break
    sleep 2
  done
  nohup ${KAFKA_BASE_DIR}/start.sh > ${KAFKA_LOG_DIR}/kafka-stdout.log 2>&1 &
fi
EOF
)"
  run ssh -o BatchMode=yes -o ConnectTimeout=10 "${SSH_USER}@${host}" "${remote_script}"
  log_ok "${host} 重啟指令已送出"
}

# -----------------------------------------------------------------------------
# 主流程
# -----------------------------------------------------------------------------
preflight_gate

if [[ "${LOCAL_MODE}" == "true" ]]; then
  restart_local
  wait_fully_replicated
else
  IFS=',' read -ra HOST_ARR <<<"${HOSTS}"
  TOTAL="${#HOST_ARR[@]}"
  IDX=0
  for h in "${HOST_ARR[@]}"; do
    IDX=$(( IDX + 1 ))
    section "[${IDX}/${TOTAL}] ${h}"
    restart_remote "${h}"

    log_info "等待 ${h} 重新加入叢集"
    sleep 10
    wait_for_cluster 180 || die "叢集在 ${h} 重啟後無法連線，中止滾動重啟"
    wait_fully_replicated || die "在 ${h} 之後未能完全同步，中止滾動重啟"

    if (( IDX < TOTAL )); then
      log_info "緩衝 ${WAIT_SECONDS}s 再處理下一台"
      sleep "${WAIT_SECONDS}"
    fi
  done
fi

# -----------------------------------------------------------------------------
section "重新平衡 leader"
# 重啟後 leader 會集中在還沒重啟的 broker 上，把它們換回 preferred replica
if [[ -x "${KAFKA_HOME}/bin/kafka-leader-election.sh" ]]; then
  run "$(kafka_bin kafka-leader-election.sh)" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      ${KAFKA_CLIENT_CONFIG:+--admin.config "${KAFKA_CLIENT_CONFIG}"} \
      --election-type PREFERRED --all-topic-partitions 2>&1 \
    | grep -v 'Picked up' || log_info "沒有需要換回的 leader"
fi

section "結果"
"${REPO_ROOT}/scripts/ops/health-check.sh" || true
log_ok "滾動重啟完成"
