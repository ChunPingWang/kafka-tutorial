#!/usr/bin/env bash
# =============================================================================
# perf-test.sh - 效能基準測試（容量規劃用）
#
# 使用 Kafka 內建的 kafka-producer-perf-test.sh / kafka-consumer-perf-test.sh，
# 跑出這座叢集在不同 acks 與訊息大小下的吞吐與延遲，作為容量規劃的依據。
#
# 用法：
#   ./scripts/test/perf-test.sh                       # 預設情境
#   ./scripts/test/perf-test.sh --records 1000000 --size 1024
#   ./scripts/test/perf-test.sh --quick               # 快速版（CI 用）
#
# 選項：
#   --records N     每個情境送出的訊息數（預設 200000）
#   --size N        訊息大小 bytes（預設 1024）
#   --partitions N  測試 topic 的 partition 數（預設 6）
#   --throughput N  限流，-1 表示全速（預設 -1）
#   --quick         快速模式：records=20000
#   --keep          測試後保留 topic 與結果
#
# 輸出：結果表格 + CSV（${KAFKA_BASE_DIR}/perf-results/）
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

RECORDS=200000
MSG_SIZE=1024
PARTITIONS=6
THROUGHPUT=-1
KEEP=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --records)    RECORDS="$2"; shift 2 ;;
    --size)       MSG_SIZE="$2"; shift 2 ;;
    --partitions) PARTITIONS="$2"; shift 2 ;;
    --throughput) THROUGHPUT="$2"; shift 2 ;;
    --quick)      RECORDS=20000; shift ;;
    --keep)       KEEP=true; shift ;;
    -h|--help)    sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

TOPIC="perf-test-$(date +%s)"
RESULT_DIR="${KAFKA_BASE_DIR}/perf-results"
CSV="${RESULT_DIR}/perf-$(timestamp).csv"
mkdir -p "${RESULT_DIR}"

cleanup() {
  local rc=$?
  if [[ "${KEEP}" != "true" ]]; then
    kafka_topics --delete --topic "${TOPIC}" >/dev/null 2>&1 || true
  fi
  exit "${rc}"
}
trap cleanup EXIT

cluster_ready || die "叢集無法連線：${BOOTSTRAP_SERVERS}"

RF="$("$(kafka_bin kafka-broker-api-versions.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" 2>/dev/null \
      | grep -cE '^\S+:[0-9]+ \(id:' || true)"
RF="${RF:-1}"
(( RF > 3 )) && RF=3
(( RF < 1 )) && RF=1

section "效能測試設定"
cat >&2 <<EOF
  bootstrap    : ${BOOTSTRAP_SERVERS}
  topic        : ${TOPIC}
  partitions   : ${PARTITIONS}
  replication  : ${RF}
  訊息數/情境  : ${RECORDS}
  訊息大小     : ${MSG_SIZE} bytes
  資料量/情境  : $(human_bytes $(( RECORDS * MSG_SIZE )))
  結果 CSV     : ${CSV}
EOF

log_info "建立測試 topic"
kafka_topics --create --topic "${TOPIC}" --partitions "${PARTITIONS}" \
             --replication-factor "${RF}" >/dev/null

echo "scenario,acks,linger_ms,batch_size,compression,records,throughput_rec_s,throughput_mb_s,avg_latency_ms,p95_ms,p99_ms,p999_ms" > "${CSV}"

# -----------------------------------------------------------------------------
# 執行一個 producer 情境
#   run_producer <情境名> <acks> <linger.ms> <batch.size> <compression>
# -----------------------------------------------------------------------------
run_producer() {
  local name="$1" acks="$2" linger="$3" batch="$4" comp="$5"
  section "Producer 情境：${name}"
  printf '  acks=%s linger.ms=%s batch.size=%s compression=%s\n' "${acks}" "${linger}" "${batch}" "${comp}" >&2

  local out
  out="$("$(kafka_bin kafka-producer-perf-test.sh)" \
      --topic "${TOPIC}" \
      --num-records "${RECORDS}" \
      --record-size "${MSG_SIZE}" \
      --throughput "${THROUGHPUT}" \
      --producer-props \
        bootstrap.servers="${BOOTSTRAP_SERVERS}" \
        acks="${acks}" \
        linger.ms="${linger}" \
        batch.size="${batch}" \
        compression.type="${comp}" \
      ${KAFKA_CLIENT_CONFIG:+--producer.config "${KAFKA_CLIENT_CONFIG}"} \
      2>&1 | grep -v 'Picked up' || true)"

  # 最後一行格式：
  # N records sent, X records/sec (Y MB/sec), Z ms avg latency, W ms max latency,
  #   a ms 50th, b ms 95th, c ms 99th, d ms 99.9th.
  local last
  last="$(printf '%s\n' "${out}" | grep 'records sent' | tail -1)"
  printf '  %s\n' "${last}" >&2

  local rec_s mb_s avg p95 p99 p999
  rec_s="$(sed -n 's/.*, \([0-9.]*\) records\/sec.*/\1/p'      <<<"${last}")"
  mb_s="$( sed -n 's/.*(\([0-9.]*\) MB\/sec).*/\1/p'           <<<"${last}")"
  avg="$(  sed -n 's/.*, \([0-9.]*\) ms avg latency.*/\1/p'    <<<"${last}")"
  p95="$(  sed -n 's/.*, \([0-9.]*\) ms 95th.*/\1/p'           <<<"${last}")"
  p99="$(  sed -n 's/.*, \([0-9.]*\) ms 99th.*/\1/p'           <<<"${last}")"
  p999="$( sed -n 's/.*, \([0-9.]*\) ms 99.9th.*/\1/p'         <<<"${last}")"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${name}" "${acks}" "${linger}" "${batch}" "${comp}" "${RECORDS}" \
    "${rec_s:-NA}" "${mb_s:-NA}" "${avg:-NA}" "${p95:-NA}" "${p99:-NA}" "${p999:-NA}" >> "${CSV}"
}

# 四種常見權衡：
#   最快但可能掉資料 -> 最安全 -> 安全且吞吐佳（批次）-> 安全 + 壓縮
run_producer "acks0-最快"        0    0   16384  none
run_producer "acks1-折衷"        1    0   16384  none
run_producer "acksAll-最安全"    all  0   16384  none
run_producer "acksAll-批次調校"  all  20  131072 none
if [[ "${SKIP_COMPRESSION:-false}" != "true" ]]; then
  run_producer "acksAll-lz4壓縮"  all  20  131072 lz4
  run_producer "acksAll-zstd壓縮" all  20  131072 zstd
fi

# -----------------------------------------------------------------------------
section "Consumer 情境"
CONSUMER_OUT="$("$(kafka_bin kafka-consumer-perf-test.sh)" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --topic "${TOPIC}" \
    --messages "${RECORDS}" \
    --group "perf-consumer-$(date +%s)" \
    ${KAFKA_CLIENT_CONFIG:+--consumer.config "${KAFKA_CLIENT_CONFIG}"} \
    2>&1 | grep -v 'Picked up' || true)"
printf '%s\n' "${CONSUMER_OUT}" | sed 's/^/  /' >&2

CONS_LINE="$(printf '%s\n' "${CONSUMER_OUT}" | tail -1)"
CONS_MB="$(awk -F, '{gsub(/ /,"",$4); print $4}' <<<"${CONS_LINE}")"
CONS_RATE="$(awk -F, '{gsub(/ /,"",$6); print $6}' <<<"${CONS_LINE}")"
printf 'consumer,-,-,-,-,%s,%s,%s,NA,NA,NA,NA\n' "${RECORDS}" "${CONS_RATE:-NA}" "${CONS_MB:-NA}" >> "${CSV}"

# -----------------------------------------------------------------------------
section "結果彙總"
if command -v column >/dev/null 2>&1; then
  column -t -s, "${CSV}" | sed 's/^/  /'
else
  sed 's/^/  /' "${CSV}"
fi

cat >&2 <<'EOF'

  怎麼讀這張表：
    - acks=0 到 acksAll 的吞吐落差，就是「可靠性的價格」。
      落差過大（>50%）通常代表副本同步是瓶頸：檢查網路頻寬與磁碟。
    - linger.ms + 大 batch.size 幾乎一定會提升吞吐，代價是多幾毫秒延遲。
    - 壓縮換的是網路與磁碟，付的是 CPU。訊息若是 JSON/文字，zstd 常有 3-5 倍壓縮率。
    - p99 / p999 才是使用者實際感受到的延遲；平均值會騙人。
    - 容量規劃：用 acksAll（正式環境設定）的 MB/sec 除以 2 當作安全水位。
EOF
log_ok "結果已存到 ${CSV}"
