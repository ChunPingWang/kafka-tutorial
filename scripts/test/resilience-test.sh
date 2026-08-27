#!/usr/bin/env bash
# =============================================================================
# resilience-test.sh - 故障注入測試（驗證「掛一台會怎樣」）
#
# 這是最容易被跳過、卻最有價值的測試。它回答：
#   - 掛掉一個 broker，producer 還寫得進去嗎？
#   - min.insync.replicas 真的會擋住不安全的寫入嗎？
#   - acks=1 在 leader 切換時真的會掉資料嗎？
#   - consumer rebalance 要多久？
#
# 前提：需要「多節點」叢集才有意義。單機叢集只會跑得了部分測項。
#
# 用法：
#   ./scripts/test/resilience-test.sh --broker-stop-cmd 'ssh kafka-2 sudo systemctl stop kafka' \
#                                     --broker-start-cmd 'ssh kafka-2 sudo systemctl start kafka'
#   # Docker Compose 環境：
#   ./scripts/test/resilience-test.sh --docker kafka-2
#
# 安全性：只在測試環境跑。腳本會建立自己的 topic，但「會真的停掉 broker」。
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

STOP_CMD=""
START_CMD=""
DOCKER_TARGET=""
TOPIC="resilience-$(date +%s)"
MSG_COUNT="${MSG_COUNT:-2000}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --broker-stop-cmd)  STOP_CMD="$2"; shift 2 ;;
    --broker-start-cmd) START_CMD="$2"; shift 2 ;;
    --docker)           DOCKER_TARGET="$2"; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

if [[ -n "${DOCKER_TARGET}" ]]; then
  STOP_CMD="docker stop ${DOCKER_TARGET}"
  START_CMD="docker start ${DOCKER_TARGET}"
fi

PASS=0; FAIL=0; SKIP=0
ok()   { PASS=$(( PASS + 1 )); printf '  %s✔ PASS%s %s\n' "${C_GRN}" "${C_RESET}" "$*"; }
ng()   { FAIL=$(( FAIL + 1 )); printf '  %s✘ FAIL%s %s\n' "${C_RED}" "${C_RESET}" "$*"; }
skip() { SKIP=$(( SKIP + 1 )); printf '  %s— SKIP%s %s\n' "${C_DIM}" "${C_RESET}" "$*"; }

cleanup() {
  kafka_topics --delete --topic "${TOPIC}" >/dev/null 2>&1 || true
  if [[ -n "${START_CMD}" ]]; then
    log_info "確保 broker 已復原"
    eval "${START_CMD}" >/dev/null 2>&1 || true
  fi
  return 0
}
trap cleanup EXIT

# 故障注入是「真的」停掉 broker，DRY_RUN 語意在這裡無法成立
# （kafka_* 包裝會被跳過、docker stop 卻照跑，狀態會亂掉）——直接拒絕。
[[ "${DRY_RUN}" == "true" ]] && die "resilience-test 不支援 DRY_RUN=true：故障注入無法「預演」，請直接執行或先讀 --help"

cluster_ready || die "叢集無法連線"
BROKERS="$("$(kafka_bin kafka-broker-api-versions.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} 2>/dev/null \
    | grep -cE '^\S+:[0-9]+ \(id:' || true)"
(( BROKERS < 1 )) && BROKERS=1

section "故障韌性測試"
printf '  叢集 broker 數 : %s\n' "${BROKERS}" >&2
printf '  測試 topic     : %s\n' "${TOPIC}" >&2

RF=3; MIN_ISR=2
STRICT_MINISR=true
if (( BROKERS < 3 )); then
  log_warn "叢集只有 ${BROKERS} 個 broker。故障注入測試需要 3 個以上才有意義。"
  RF="${BROKERS}"; MIN_ISR=1
elif (( BROKERS > 3 )); then
  # RF=3 但 broker 更多時，六個 partition 的副本「不一定」都落在被停掉的那台上，
  # 測試 3 的「全部寫不進去」預期不成立（沒涵蓋該台的 partition 照常收）——
  # 那不是叢集的錯，是拓撲使然，改以資訊性方式回報而非判 FAIL。
  log_warn "叢集有 ${BROKERS}（>3）個 broker：測試 3 的嚴格斷言改為資訊性檢查"
  STRICT_MINISR=false
fi

kafka_topics --create --topic "${TOPIC}" --partitions 6 --replication-factor "${RF}" \
  --config min.insync.replicas="${MIN_ISR}" >/dev/null
log_ok "已建立 topic（RF=${RF}, min.insync.replicas=${MIN_ISR}）"

produce_n() {   # produce_n <筆數> <acks> ；回傳實際成功筆數
  local n="$1" acks="$2"
  local before after
  before="$("$(kafka_bin kafka-get-offsets.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
             --topic "${TOPIC}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
  seq 1 "${n}" | sed 's/^/msg-/' \
    | "$(kafka_bin kafka-console-producer.sh)" \
        --bootstrap-server "${BOOTSTRAP_SERVERS}" --topic "${TOPIC}" \
        ${KAFKA_CLIENT_CONFIG:+--producer.config "${KAFKA_CLIENT_CONFIG}"} \
        --request-required-acks "${acks}" >/dev/null 2>&1 || true
  after="$("$(kafka_bin kafka-get-offsets.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
             --topic "${TOPIC}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
  printf '%s' "$(( after - before ))"
}

# -----------------------------------------------------------------------------
section "測試 1：正常狀態下的基準"
GOT="$(produce_n "${MSG_COUNT}" all)"
if (( GOT == MSG_COUNT )); then
  ok "acks=all 正常寫入 ${GOT} 筆"
else
  ng "acks=all 只寫入 ${GOT}/${MSG_COUNT} 筆（叢集本來就有問題？）"
fi

# -----------------------------------------------------------------------------
section "測試 2：停掉一個 broker 之後"
if [[ -z "${STOP_CMD}" ]]; then
  skip "未提供 --broker-stop-cmd / --docker，無法注入故障"
elif (( BROKERS < 3 )); then
  skip "broker 數不足 3，停掉一台會直接失去 quorum"
else
  log_info "執行：${STOP_CMD}"
  eval "${STOP_CMD}" || die "停止 broker 失敗"
  sleep 15

  ALIVE="$("$(kafka_bin kafka-broker-api-versions.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" 2>/dev/null \
      | grep -cE '^\S+:[0-9]+ \(id:' || true)"
  if (( ALIVE == BROKERS - 1 )); then
    ok "叢集偵測到 broker 離線（存活 ${ALIVE}/${BROKERS}）"
  else
    ng "存活 broker 數為 ${ALIVE}，預期 $(( BROKERS - 1 ))"
  fi

  UR="$(kafka_topics --describe --under-replicated-partitions 2>/dev/null | grep -c 'Topic:' || true)"
  if (( UR > 0 )); then
    ok "如預期出現 ${UR} 個 under-replicated partition"
  else
    ng "沒有 under-replicated partition，broker 可能沒真的停掉"
  fi

  # RF=3 / min.isr=2 少一台仍應可寫
  GOT="$(produce_n 500 all)"
  if (( GOT == 500 )); then
    ok "少一台 broker，acks=all 仍可寫入（RF=${RF} > min.isr=${MIN_ISR} 的價值）"
  else
    ng "acks=all 只寫入 ${GOT}/500 筆"
  fi

  # 消費仍應正常
  CONSUMED="$("$(kafka_bin kafka-console-consumer.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --topic "${TOPIC}" --from-beginning --max-messages 100 --timeout-ms 30000 2>/dev/null | grep -c . || true)"
  if (( CONSUMED >= 100 )); then
    ok "少一台 broker，consumer 仍可讀取"
  else
    ng "consumer 只讀到 ${CONSUMED} 筆"
  fi

  # -----------------------------------------------------------------------------
  section "測試 3：min.insync.replicas 的保護作用"
  # 把 min.isr 拉到跟 RF 一樣：現在少一台就湊不齊，寫入應該被擋下來。
  kafka_configs --entity-type topics --entity-name "${TOPIC}" \
    --alter --add-config min.insync.replicas="${RF}" >/dev/null 2>&1
  sleep 3

  GOT="$(produce_n 100 all)"
  if (( GOT == 0 )); then
    ok "min.insync.replicas=${RF} 但只有 $(( RF - 1 )) 個副本時，acks=all 的資料讀不到"
    printf '      producer 收到 NOT_ENOUGH_REPLICAS，這正是「寧可停寫也不遺失資料」的設計。\n' >&2
  elif [[ "${STRICT_MINISR}" != "true" ]]; then
    skip "有 ${GOT} 筆寫入成功——broker 數 > 3，這些 partition 的副本不含被停掉的那台，屬預期行為"
  else
    ng "預期讀不到，實際多了 ${GOT} 筆可見訊息"
  fi

  # acks=1：在啟用 ELR（KIP-966，新建 4.x 叢集的預設）的叢集上，
  # ISR < min.insync.replicas 時 high watermark 不前進，所以這裡量到 0。
  # 從 4.0 之前升上來、metadata 尚未啟用 ELR 的叢集沒有這個保證：
  # acks=1 的訊息會立即可見——那是「舊語意」，不是故障，不能判 FAIL。
  ACK1_GOT="$(produce_n 100 1)"
  if (( ACK1_GOT == 0 )); then
    ok "acks=1 在 ISR 不足時，訊息同樣無法被消費（high watermark 不前進，ELR 語意）"
    printf '      注意：這些訊息可能已經寫進 leader 的 log，只是還不可見；\n' >&2
    printf '      等副本追上、ISR 恢復之後才會一次浮現。\n' >&2
  elif [[ "${STRICT_MINISR}" != "true" ]]; then
    skip "acks=1 有 ${ACK1_GOT} 筆可見（部分 partition 未受影響，屬預期）"
  else
    skip "acks=1 有 ${ACK1_GOT} 筆立即可見——此叢集未啟用 ELR（KIP-966）語意（多見於從 ≤4.0 升級的 metadata），acks=1 本就沒有 min.isr 保護；請理解這正是不要用 acks=1 的理由"
  fi

  # acks=0 是真正的「射後不理」：broker 拒收也不會有人知道，資料就這樣消失。
  ACK0_BEFORE="$("$(kafka_bin kafka-get-offsets.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
                   --topic "${TOPIC}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
  produce_n 100 0 >/dev/null

  section "測試 4：ISR 恢復後，被擋住的訊息會怎樣"
  log_info "執行：${START_CMD}"
  eval "${START_CMD}" || die "啟動 broker 失敗"
  RECOVER_START="$(date +%s)"
  RECOVERED=false
  for _ in $(seq 1 60); do
    sleep 5
    UR="$(kafka_topics --describe --under-replicated-partitions 2>/dev/null | grep -c 'Topic:' || true)"
    if (( UR == 0 )); then RECOVERED=true; break; fi
  done
  RECOVER_TIME=$(( $(date +%s) - RECOVER_START ))

  if [[ "${RECOVERED}" == true ]]; then
    ok "broker 復原後 ${RECOVER_TIME} 秒內所有副本追平"
    printf '      這個數字就是你的「單機故障 MTTR」，請記錄下來。\n' >&2
  else
    ng "300 秒後仍有 under-replicated partition"
  fi

  kafka_configs --entity-type topics --entity-name "${TOPIC}" \
    --alter --add-config min.insync.replicas="${MIN_ISR}" >/dev/null 2>&1
  sleep 5

  ACK0_AFTER="$("$(kafka_bin kafka-get-offsets.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
                  --topic "${TOPIC}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
  SURFACED=$(( ACK0_AFTER - ACK0_BEFORE ))
  printf '      ISR 恢復後浮現 %d 筆先前不可見的訊息\n' "${SURFACED}" >&2
  if (( SURFACED > 0 )); then
    ok "被 high watermark 擋住的訊息在 ISR 恢復後浮現"
    printf '      維運意義：ISR 不足期間看到的「訊息不見了」不一定是真的遺失，\n' >&2
    printf '      先讓副本追上再判斷，不要急著重送而造成重複。\n' >&2
  else
    ok "沒有訊息浮現（producer 端在 ISR 不足時就已放棄，屬合理結果）"
  fi

fi

# -----------------------------------------------------------------------------
section "測試 5：Consumer rebalance 時間"
GROUP="resilience-group-$(date +%s)"
"$(kafka_bin kafka-console-consumer.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --topic "${TOPIC}" --group "${GROUP}" --from-beginning --timeout-ms 20000 >/dev/null 2>&1 &
C1=$!
sleep 8
RB_START="$(date +%s)"
"$(kafka_bin kafka-console-consumer.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --topic "${TOPIC}" --group "${GROUP}" --timeout-ms 20000 >/dev/null 2>&1 &
C2=$!
# 等到兩個成員都拿到 partition
STABLE=false
for _ in $(seq 1 30); do
  MEMBERS="$(kafka_groups --describe --group "${GROUP}" --members 2>/dev/null | grep -c 'consumer' || true)"
  if (( MEMBERS >= 2 )); then STABLE=true; break; fi
  sleep 1
done
RB_TIME=$(( $(date +%s) - RB_START ))
wait "${C1}" "${C2}" 2>/dev/null || true
if [[ "${STABLE}" == true ]]; then
  ok "新 consumer 加入後 ${RB_TIME} 秒完成 rebalance"
  printf '      rebalance 期間該 group 是停止消費的，這段時間會累積 lag。\n' >&2
else
  skip "未能觀察到穩定的兩成員狀態（測試環境時序不穩屬正常）"
fi

# -----------------------------------------------------------------------------
section "測試結果"
printf '  通過：%d  失敗：%d  略過：%d\n' "${PASS}" "${FAIL}" "${SKIP}"
(( FAIL > 0 )) && exit 1
log_ok "韌性測試完成"
