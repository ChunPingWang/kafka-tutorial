#!/usr/bin/env bash
# =============================================================================
# failover.sh - 災難切換（把服務從主叢集切到備援叢集）
#
# 這支腳本負責「Kafka 這一側」的切換動作，並產生一份事件記錄。
# 它「不會」自動改你的應用程式設定 —— 那一步必須由你或部署系統執行，
# 腳本會把需要改的內容明確列出來。
#
# 切換流程：
#   1. 前置檢查：備援叢集健康、複寫落差可接受
#   2. 快照現況（RPO 證據，事後檢討要用）
#   3. 停止 MM2 正向複寫（避免主叢集復活後把舊資料蓋回來）
#   4. 確認 consumer offset 已翻譯到備援叢集
#   5. 產生 client 切換指引
#   6. （選用）建立反向複寫，為日後 failback 鋪路
#   7. 寫入事件記錄
#
# 用法：
#   ./scripts/dr/failover.sh --auto                    # 從 mm2/topology 讀設定
#   ./scripts/dr/failover.sh --auto --force            # 主叢集已死，跳過落差檢查
#   ./scripts/dr/failover.sh --auto --setup-reverse    # 同時建立反向複寫
#   ./scripts/dr/failover.sh --auto --drill            # 演練模式：只檢查不動作
#
# 重要：--drill 應該每季至少跑一次。沒演練過的 DR 計畫不算 DR 計畫。
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

SRC_ALIAS=""; SRC=""
DST_ALIAS=""; DST=""
FORCE=false
DRILL=false
SETUP_REVERSE=false
MAX_ACCEPTABLE_LAG="${MAX_ACCEPTABLE_LAG:-1000}"
IDENTITY_POLICY=false
MM2_CONFIG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --auto)
      TOPO="${KAFKA_BASE_DIR}/mm2/topology"
      [[ -f "${TOPO}" ]] || die "找不到 ${TOPO}，請先執行 setup-mirrormaker.sh"
      # shellcheck disable=SC1090
      source "${TOPO}"
      SRC_ALIAS="${SOURCE_ALIAS}"; SRC="${SOURCE_BOOTSTRAP}"
      DST_ALIAS="${TARGET_ALIAS}"; DST="${TARGET_BOOTSTRAP}"
      MM2_CONFIG="${CONFIG:-}"
      shift ;;
    --source-alias) SRC_ALIAS="$2"; shift 2 ;;
    --source)       SRC="$2"; shift 2 ;;
    --target-alias) DST_ALIAS="$2"; shift 2 ;;
    --target)       DST="$2"; shift 2 ;;
    --force)        FORCE=true; shift ;;
    --drill)        DRILL=true; shift ;;
    --setup-reverse) SETUP_REVERSE=true; shift ;;
    -h|--help)      sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

[[ -n "${SRC}" && -n "${DST}" && -n "${SRC_ALIAS}" ]] || die "參數不足，或改用 --auto"

KT="${KAFKA_HOME}/bin/kafka-topics.sh"
KGO="${KAFKA_HOME}/bin/kafka-get-offsets.sh"
KCG="${KAFKA_HOME}/bin/kafka-consumer-groups.sh"

INCIDENT_ID="failover-$(timestamp)"
INCIDENT_DIR="${KAFKA_BASE_DIR}/incidents/${INCIDENT_ID}"
mkdir -p "${INCIDENT_DIR}"

remote_name() {
  if [[ "${IDENTITY_POLICY}" == "true" ]]; then printf '%s' "$1"; else printf '%s.%s' "${SRC_ALIAS}" "$1"; fi
}

if [[ "${DRILL}" == "true" ]]; then
  section "★ 演練模式（--drill）：只做檢查，不會停止複寫或改變任何狀態"
fi

# -----------------------------------------------------------------------------
section "1/7 前置檢查"
BOOTSTRAP_SERVERS="${DST}" cluster_ready || die "備援叢集 ${DST} 無法連線，無法切換"
log_ok "備援叢集可連線"

SRC_ALIVE=false
BOOTSTRAP_SERVERS="${SRC}" cluster_ready && SRC_ALIVE=true
if [[ "${SRC_ALIVE}" == true ]]; then
  log_warn "主叢集 ${SRC} 仍然可連線。"
  log_warn "若這不是計畫性切換，請先確認主叢集真的不能用，避免造成雙寫（split-brain）。"
else
  log_info "主叢集 ${SRC} 無法連線（符合災難情境）"
fi

# 備援叢集本身健康嗎
DR_SEVERITY=0
BOOTSTRAP_SERVERS="${DST}" "${REPO_ROOT}/scripts/ops/health-check.sh" \
  > "${INCIDENT_DIR}/dr-health-before.txt" 2>&1 || DR_SEVERITY=$?
sed 's/^/  /' "${INCIDENT_DIR}/dr-health-before.txt" >&2
if (( DR_SEVERITY >= 2 )) && [[ "${FORCE}" != "true" ]]; then
  die "備援叢集本身狀態嚴重異常，切過去也撐不住。用 --force 可強制繼續。"
fi

# -----------------------------------------------------------------------------
section "2/7 快照現況（RPO 證據）"
{
  echo "incident_id=${INCIDENT_ID}"
  echo "started_at=$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "operator=$(id -un)@$(hostname -f 2>/dev/null || hostname)"
  echo "source_alias=${SRC_ALIAS}"
  echo "source_bootstrap=${SRC}"
  echo "source_reachable=${SRC_ALIVE}"
  echo "target_alias=${DST_ALIAS}"
  echo "target_bootstrap=${DST}"
  echo "drill=${DRILL}"
  echo "forced=${FORCE}"
} > "${INCIDENT_DIR}/incident.txt"

TOTAL_LAG=0
if [[ "${SRC_ALIVE}" == true ]]; then
  printf '  %-32s %10s %10s %8s\n' "TOPIC" "來源" "備援" "落差" | tee "${INCIDENT_DIR}/lag-snapshot.txt"
  while IFS= read -r t; do
    [[ -z "${t}" || "${t}" == __* || "${t}" == mm2-* || "${t}" == heartbeats || "${t}" == *".internal" ]] && continue
    RT="$(remote_name "${t}")"
    S="$("${KGO}" --bootstrap-server "${SRC}" --topic "${t}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
    D="$("${KGO}" --bootstrap-server "${DST}" --topic "${RT}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
    LAG=$(( S - D )); (( LAG < 0 )) && LAG=0
    TOTAL_LAG=$(( TOTAL_LAG + LAG ))
    printf '  %-32s %10s %10s %8s\n' "${t}" "${S}" "${D}" "${LAG}" | tee -a "${INCIDENT_DIR}/lag-snapshot.txt"
  done < <("${KT}" --bootstrap-server "${SRC}" --list 2>/dev/null | sort)
  echo "measured_rpo_messages=${TOTAL_LAG}" >> "${INCIDENT_DIR}/incident.txt"
  printf '\n  總落差（估算 RPO）：%d 筆\n' "${TOTAL_LAG}" >&2

  if (( TOTAL_LAG > MAX_ACCEPTABLE_LAG )) && [[ "${FORCE}" != "true" ]]; then
    log_error "落差 ${TOTAL_LAG} 筆超過可接受值 ${MAX_ACCEPTABLE_LAG} 筆。"
    log_error "若主叢集還活著，建議先讓 MM2 追平再切；確定要接受資料損失請加 --force。"
    exit 1
  fi
else
  echo "measured_rpo_messages=unknown_source_down" >> "${INCIDENT_DIR}/incident.txt"
  log_warn "主叢集不可連線，無法量測實際落差"
fi

"${KT}" --bootstrap-server "${DST}" --describe > "${INCIDENT_DIR}/dr-topics.txt" 2>/dev/null || true

# -----------------------------------------------------------------------------
section "3/7 停止正向複寫"
PID_FILE="${KAFKA_BASE_DIR}/mm2/mm2-${SRC_ALIAS}-to-${DST_ALIAS}.pid"
if [[ "${DRILL}" == "true" ]]; then
  log_info "[演練] 會停止 ${PID_FILE} 的 MM2 行程"
elif [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
  MM2_PID="$(cat "${PID_FILE}")"
  log_info "停止 MM2（pid ${MM2_PID}）"
  kill "${MM2_PID}"
  for _ in $(seq 1 30); do
    kill -0 "${MM2_PID}" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "${MM2_PID}" 2>/dev/null; then
    log_warn "MM2 未在 30 秒內結束，送出 SIGKILL"
    kill -9 "${MM2_PID}" 2>/dev/null || true
  fi
  rm -f "${PID_FILE}"
  log_ok "正向複寫已停止"
  echo "mm2_stopped=true" >> "${INCIDENT_DIR}/incident.txt"
else
  log_info "找不到執行中的 MM2 行程（可能已隨主機一起掛掉）"
  echo "mm2_stopped=not_running" >> "${INCIDENT_DIR}/incident.txt"
fi

# -----------------------------------------------------------------------------
section "4/7 確認 consumer offset 已翻譯"
DST_GROUPS="$("${KCG}" --bootstrap-server "${DST}" --list 2>/dev/null | grep -v '^$' || true)"
READY=0; NOT_READY=""
if [[ -n "${DST_GROUPS}" ]]; then
  while IFS= read -r g; do
    [[ -z "${g}" || "${g}" == console-consumer-* || "${g}" == connect-* ]] && continue
    DESC="$("${KCG}" --bootstrap-server "${DST}" --describe --group "${g}" 2>/dev/null || true)"
    printf '%s\n' "${DESC}" > "${INCIDENT_DIR}/group-${g//\//_}.txt"
    N="$(printf '%s\n' "${DESC}" | awk '$4 ~ /^[0-9]+$/' | grep -c . || true)"
    if (( N > 0 )); then
      printf '  %s✔%s %-30s %s 個 partition 有 offset\n' "${C_GRN}" "${C_RESET}" "${g}" "${N}"
      READY=$(( READY + 1 ))
    else
      NOT_READY="${NOT_READY} ${g}"
    fi
  done <<<"${DST_GROUPS}"
fi
printf '  已翻譯 %d 個 group\n' "${READY}" >&2
if [[ -n "${NOT_READY}" ]]; then
  log_warn "下列 group 在備援端沒有 offset，切過去會依 auto.offset.reset 決定起點：${NOT_READY# }"
  log_warn "若 auto.offset.reset=latest，這些 consumer 會漏掉切換前積壓的訊息。"
fi
echo "groups_translated=${READY}" >> "${INCIDENT_DIR}/incident.txt"

# -----------------------------------------------------------------------------
section "5/7 Client 切換指引"
CUTOVER="${INCIDENT_DIR}/cutover-instructions.txt"
{
  echo "================================================================"
  echo " Client 切換指引  incident=${INCIDENT_ID}"
  echo "================================================================"
  echo
  echo "1) 把所有 producer / consumer 的 bootstrap.servers 改為："
  echo "     bootstrap.servers=${DST}"
  echo
  echo "2) Topic 名稱："
  if [[ "${IDENTITY_POLICY}" == "true" ]]; then
    echo "     使用 IdentityReplicationPolicy，topic 名稱不變，不需修改。"
  else
    echo "     使用 DefaultReplicationPolicy，備援端 topic 多了 '${SRC_ALIAS}.' 前綴。"
    echo "     Consumer 需要改讀："
    while IFS= read -r t; do
      [[ -z "${t}" || "${t}" == __* || "${t}" == mm2-* || "${t}" == heartbeats || "${t}" == *".internal" ]] && continue
      echo "       ${t}  ->  ${SRC_ALIAS}.${t}"
    done < <("${KT}" --bootstrap-server "${DST}" --list 2>/dev/null \
             | sed -n "s/^${SRC_ALIAS}\\.//p" | sort)
    echo
    echo "     Producer 要寫到「原本的名字」（不加前綴），否則資料會被當成再次複寫的內容。"
    echo "     若備援端還沒有這些原名 topic，請先建立。"
  fi
  echo
  echo "3) 啟動順序：先啟 consumer，確認 lag 正常後再啟 producer。"
  echo "   這樣可以在還沒有新流量時先驗證讀取路徑。"
  echo
  echo "4) 驗證："
  echo "     BOOTSTRAP_SERVERS=${DST} ./scripts/ops/health-check.sh"
  echo "     BOOTSTRAP_SERVERS=${DST} ./scripts/test/smoke-test.sh"
  echo
  echo "5) 主叢集復活後「不要」直接把流量切回去。"
  echo "   正確做法是先建立反向複寫（${DST_ALIAS} -> ${SRC_ALIAS}），等追平後"
  echo "   再安排計畫性 failback。"
} > "${CUTOVER}"
sed 's/^/  /' "${CUTOVER}" >&2

# -----------------------------------------------------------------------------
section "6/7 反向複寫（為 failback 鋪路）"
if [[ "${DRILL}" == "true" ]]; then
  log_info "[演練] 略過"
elif [[ "${SETUP_REVERSE}" == "true" ]]; then
  if [[ "${SRC_ALIVE}" == true ]]; then
    log_info "建立 ${DST_ALIAS} -> ${SRC_ALIAS} 反向複寫"
    "${REPO_ROOT}/scripts/dr/setup-mirrormaker.sh" \
      --source-alias "${DST_ALIAS}" --source "${DST}" \
      --target-alias "${SRC_ALIAS}" --target "${SRC}" \
      --rf "${MM2_RF:-3}" --start || log_warn "反向複寫建立失敗，可稍後手動處理"
  else
    log_warn "主叢集尚未復活，無法建立反向複寫。復活後請執行："
    printf '    ./scripts/dr/setup-mirrormaker.sh --source-alias %s --source %s \\\n' "${DST_ALIAS}" "${DST}" >&2
    printf '        --target-alias %s --target %s --start\n' "${SRC_ALIAS}" "${SRC}" >&2
  fi
else
  log_info "未指定 --setup-reverse，略過。日後 failback 前記得建立反向複寫。"
fi

# -----------------------------------------------------------------------------
section "7/7 事件記錄"
{
  echo "finished_at=$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "status=$( [[ "${DRILL}" == true ]] && echo drill_completed || echo failover_completed )"
} >> "${INCIDENT_DIR}/incident.txt"

BOOTSTRAP_SERVERS="${DST}" "${REPO_ROOT}/scripts/ops/health-check.sh" \
  > "${INCIDENT_DIR}/dr-health-after.txt" 2>&1 || true

cat >&2 <<EOF

  事件目錄 : ${INCIDENT_DIR}
    incident.txt              摘要與 RPO
    lag-snapshot.txt          切換當下各 topic 落差
    cutover-instructions.txt  給應用團隊的切換指引
    dr-health-before/after    切換前後的叢集健康

EOF

if [[ "${DRILL}" == "true" ]]; then
  log_ok "演練完成。請把上面的指引與事件目錄併入 DR 手冊，並記錄本次演練耗時。"
else
  log_ok "Kafka 側切換完成。接下來由應用團隊執行 cutover-instructions.txt。"
fi
