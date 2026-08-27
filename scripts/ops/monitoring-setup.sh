#!/usr/bin/env bash
# =============================================================================
# monitoring-setup.sh - 設定 JMX 指標匯出（Prometheus）
#
# Kafka 的所有內部指標都透過 JMX 暴露。要接進 Prometheus，
# 最常見的做法是掛上 jmx_exporter 這個 Java agent。
#
# 這支腳本會：
#   1. 下載 jmx_exporter javaagent
#   2. 產生 kafka 專用的指標對應設定（只收有用的，避免指標爆量）
#   3. 產生要加到 KAFKA_OPTS 的啟動參數
#   4. 產生 Prometheus scrape 設定與告警規則
#
# 用法：
#   ./scripts/ops/monitoring-setup.sh
#   ./scripts/ops/monitoring-setup.sh --port 7071
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

EXPORTER_PORT="${EXPORTER_PORT:-7071}"
EXPORTER_VERSION="${JMX_EXPORTER_VERSION:-1.0.1}"
MON_DIR="${KAFKA_BASE_DIR}/monitoring"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)    EXPORTER_PORT="$2"; shift 2 ;;
    --version) EXPORTER_VERSION="$2"; shift 2 ;;
    -h|--help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

mkdir -p "${MON_DIR}"

# -----------------------------------------------------------------------------
section "1/4 下載 jmx_exporter"
AGENT_JAR="${MON_DIR}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar"
AGENT_URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar"
if [[ -s "${AGENT_JAR}" ]]; then
  log_info "已存在，略過下載"
elif retry 3 2 -- curl -fsSL --connect-timeout 20 -o "${AGENT_JAR}.part" "${AGENT_URL}"; then
  mv "${AGENT_JAR}.part" "${AGENT_JAR}"
  log_ok "已下載 $(human_bytes "$(stat -c%s "${AGENT_JAR}")")"
else
  rm -f "${AGENT_JAR}.part"
  log_warn "下載失敗（離線環境？）。請手動下載並放到 ${AGENT_JAR}："
  printf '    %s\n' "${AGENT_URL}" >&2
fi

# -----------------------------------------------------------------------------
section "2/4 產生指標對應設定"
cat > "${MON_DIR}/kafka-jmx.yml" <<'EOF'
# jmx_exporter 設定：只收「真的會拿來看板與告警」的指標。
# 全收會產生數萬條時間序列，把 Prometheus 撐爆。
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  # ---- 最重要的四個健康指標 ----
  # under-replicated：副本沒跟上，代表有 broker 掛了或落後
  - pattern: kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value
    name: kafka_server_replicamanager_underreplicatedpartitions
    type: GAUGE
  # under-min-isr：已經低於 min.insync.replicas，acks=all 的寫入正在被拒絕
  - pattern: kafka.server<type=ReplicaManager, name=UnderMinIsrPartitionCount><>Value
    name: kafka_server_replicamanager_underminisrpartitioncount
    type: GAUGE
  # offline partition：沒有 leader，讀寫都失敗（只有 controller 會回報）
  - pattern: kafka.controller<type=KafkaController, name=OfflinePartitionsCount><>Value
    name: kafka_controller_offlinepartitionscount
    type: GAUGE
  # active controller：整個叢集加總必須永遠等於 1
  - pattern: kafka.controller<type=KafkaController, name=ActiveControllerCount><>Value
    name: kafka_controller_activecontrollercount
    type: GAUGE

  # ---- 流量 ----
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(BytesInPerSec|BytesOutPerSec|MessagesInPerSec|BytesRejectedPerSec), topic=(.+)><>Count
    name: kafka_server_brokertopicmetrics_$1_total
    labels: {topic: "$2"}
    type: COUNTER
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(BytesInPerSec|BytesOutPerSec|MessagesInPerSec|BytesRejectedPerSec)><>Count
    name: kafka_server_brokertopicmetrics_$1_total
    type: COUNTER

  # ---- 請求延遲（p99 才是使用者感受）----
  - pattern: kafka.network<type=RequestMetrics, name=(TotalTimeMs|RequestQueueTimeMs|LocalTimeMs|RemoteTimeMs|ResponseQueueTimeMs|ResponseSendTimeMs), request=(Produce|Fetch|FetchConsumer|FetchFollower|Metadata)><>(\d+)thPercentile
    name: kafka_network_requestmetrics_$1
    labels: {request: "$2", quantile: "0.$3"}
    type: GAUGE
  - pattern: kafka.network<type=RequestMetrics, name=RequestsPerSec, request=(.+), version=(.+)><>Count
    name: kafka_network_requestmetrics_requests_total
    labels: {request: "$1", version: "$2"}
    type: COUNTER

  # ---- 請求處理執行緒的閒置率：接近 0 表示 broker 快撐不住了 ----
  - pattern: kafka.server<type=KafkaRequestHandlerPool, name=RequestHandlerAvgIdlePercent><>OneMinuteRate
    name: kafka_server_requesthandler_avgidlepercent
    type: GAUGE
  - pattern: kafka.network<type=SocketServer, name=NetworkProcessorAvgIdlePercent><>Value
    name: kafka_network_processor_avgidlepercent
    type: GAUGE

  # ---- ISR 變動：頻繁 shrink/expand 代表叢集不穩 ----
  - pattern: kafka.server<type=ReplicaManager, name=(IsrShrinksPerSec|IsrExpandsPerSec)><>Count
    name: kafka_server_replicamanager_$1_total
    type: COUNTER

  # ---- log 大小與 KRaft ----
  - pattern: kafka.log<type=Log, name=Size, topic=(.+), partition=(.+)><>Value
    name: kafka_log_size_bytes
    labels: {topic: "$1", partition: "$2"}
    type: GAUGE
  - pattern: kafka.server<type=raft-metrics><>(current-state|high-watermark|current-leader|current-epoch)
    name: kafka_raft_$1
    type: GAUGE

  # ---- JVM ----
  - pattern: java.lang<type=Memory><HeapMemoryUsage>(used|max|committed)
    name: jvm_memory_heap_$1_bytes
    type: GAUGE
  - pattern: java.lang<type=GarbageCollector, name=(.+)><>(CollectionCount|CollectionTime)
    name: jvm_gc_$2
    labels: {gc: "$1"}
    type: COUNTER
  - pattern: java.lang<type=OperatingSystem><>(OpenFileDescriptorCount|MaxFileDescriptorCount|ProcessCpuLoad)
    name: jvm_os_$1
    type: GAUGE
EOF
log_ok "${MON_DIR}/kafka-jmx.yml"

# -----------------------------------------------------------------------------
section "3/4 產生啟動參數"
cat > "${MON_DIR}/kafka-opts.sh" <<EOF
# 把這一行加進 broker 的啟動環境（systemd unit 的 Environment，或 start.sh）
export KAFKA_OPTS="-javaagent:${AGENT_JAR}=${EXPORTER_PORT}:${MON_DIR}/kafka-jmx.yml \${KAFKA_OPTS:-}"
EOF
log_ok "${MON_DIR}/kafka-opts.sh"
printf '  重啟 broker 後可用以下指令確認：\n    curl -s localhost:%s/metrics | head\n' "${EXPORTER_PORT}" >&2

# -----------------------------------------------------------------------------
section "4/4 產生 Prometheus 設定與告警規則"
cat > "${MON_DIR}/prometheus-scrape.yml" <<EOF
# 併入 prometheus.yml 的 scrape_configs
- job_name: kafka
  scrape_interval: 15s
  static_configs:
    - targets:
        - kafka-1:${EXPORTER_PORT}
        - kafka-2:${EXPORTER_PORT}
        - kafka-3:${EXPORTER_PORT}
  relabel_configs:
    - source_labels: [__address__]
      regex: '([^:]+):.*'
      target_label: instance
      replacement: '\$1'
EOF

cat > "${MON_DIR}/kafka-alerts.yml" <<'EOF'
# Prometheus 告警規則。
# 原則：每一條都要「有人能處理」，否則就是雜訊，久了大家會忽略所有告警。
groups:
  - name: kafka-critical
    rules:
      # 這條最重要：沒有 leader 就等於服務中斷
      - alert: KafkaOfflinePartitions
        expr: sum(kafka_controller_offlinepartitionscount) > 0
        for: 1m
        labels: {severity: critical}
        annotations:
          summary: "有 {{ $value }} 個 partition 沒有 leader"
          description: "這些 partition 的讀寫都會失敗。檢查是否有 broker 掛掉，以及 unclean.leader.election 是否關閉。"

      # 全叢集的 active controller 加總必須恰好是 1
      - alert: KafkaNoActiveController
        expr: sum(kafka_controller_activecontrollercount) != 1
        for: 1m
        labels: {severity: critical}
        annotations:
          summary: "active controller 數量為 {{ $value }}，應該是 1"
          description: "0 = 沒有 controller，metadata 無法更新；>1 = split brain。檢查 KRaft quorum。"

      # 低於 min.insync.replicas：acks=all 的寫入正在被拒絕
      - alert: KafkaUnderMinIsr
        expr: sum(kafka_server_replicamanager_underminisrpartitioncount) > 0
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "{{ $value }} 個 partition 低於 min.insync.replicas"
          description: "producer 會收到 NOT_ENOUGH_REPLICAS。請盡快讓離線的 broker 回來。"

  - name: kafka-warning
    rules:
      - alert: KafkaUnderReplicatedPartitions
        expr: sum(kafka_server_replicamanager_underreplicatedpartitions) > 0
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "{{ $value }} 個 partition 副本不足超過 5 分鐘"
          description: "容錯能力已下降。可能是 broker 離線、網路壅塞，或磁碟太慢跟不上。"

      # 請求處理執行緒閒置率過低 = broker 過載
      - alert: KafkaRequestHandlerSaturated
        expr: kafka_server_requesthandler_avgidlepercent < 0.2
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.instance }} 的請求處理執行緒閒置率只剩 {{ $value }}"
          description: "broker 接近飽和。考慮增加 num.io.threads、加機器，或降低流量。"

      - alert: KafkaProduceLatencyHigh
        expr: kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"} > 500
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.instance }} produce p99 延遲 {{ $value }}ms"
          description: "檢查磁碟 I/O、GC 停頓、副本同步是否落後。"

      - alert: KafkaIsrFlapping
        expr: rate(kafka_server_replicamanager_isrshrinkspersec_total[15m]) > 0.05
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.instance }} 的 ISR 頻繁收縮"
          description: "副本反覆進出 ISR，通常是網路不穩、GC 停頓過長，或 replica.lag.time.max.ms 設太小。"

      # 磁碟寫滿監控需要「檔案系統容量」，這是 node_exporter 的職責，
      # Kafka 的 JMX 只知道 log 大小、不知道磁碟多大。
      # 前提：已部署 node_exporter（正式環境本來就該有）。
      # ★ 把 mountpoint="/data" 換成你的 Kafka 資料碟掛載點。
      - alert: KafkaDiskFillingUp
        expr: |
          predict_linear(node_filesystem_avail_bytes{mountpoint="/data"}[6h], 24*3600) < 0
        for: 30m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.instance }} 依目前寫入速度，24 小時內資料碟將寫滿"
          description: "調整 retention，或擴充磁碟。Kafka 磁碟寫滿會直接停止服務。"

      # 沒有 node_exporter 時的退而求其次：整機 log 寫入速率異常
      # （抓的是「不正常的成長」，不是磁碟容量——兩者都要有才完整）
      - alert: KafkaLogWriteSurge
        expr: sum by (instance) (rate(kafka_log_size_bytes[30m])) > 50 * 1024 * 1024
        for: 30m
        labels: {severity: info}
        annotations:
          summary: "{{ $labels.instance }} 的 log 寫入速率持續超過 50MB/s"
          description: "確認是否為預期流量；持續高寫入請對照磁碟剩餘空間。"

      - alert: KafkaJvmGcPauseHigh
        expr: rate(jvm_gc_collectiontime[5m]) / 1000 > 0.1
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.instance }} GC 佔用超過 10% 的時間"
          description: "heap 可能設太大或太小。Kafka 建議 heap 6-8G，其餘記憶體留給 page cache。"
EOF
log_ok "${MON_DIR}/prometheus-scrape.yml 、 kafka-alerts.yml"

section "完成"
cat >&2 <<EOF
  監控檔案目錄：${MON_DIR}

  接下來：
    1. 把 ${MON_DIR}/kafka-opts.sh 的 KAFKA_OPTS 加到每台 broker 的啟動環境
    2. 滾動重啟：./scripts/ops/rolling-restart.sh --hosts kafka-1,kafka-2,kafka-3
    3. 把 prometheus-scrape.yml 併入 prometheus.yml
    4. 把 kafka-alerts.yml 放進 Prometheus 的 rule_files

  沒有 Prometheus 也能看單一指標（直接打 jmx_exporter 的 HTTP 端點）：
    curl -s localhost:${EXPORTER_PORT}/metrics | grep -i underreplicated
EOF
