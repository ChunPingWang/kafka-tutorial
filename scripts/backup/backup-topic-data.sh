#!/usr/bin/env bash
# =============================================================================
# backup-topic-data.sh - 訊息層級的匯出與匯入
#
# 什麼時候該用這支腳本：
#   ✔ 匯出小量關鍵 topic（設定檔類、對帳基準、compacted topic 快照）
#   ✔ 跨環境搬資料（正式 -> 測試）
#   ✔ 誤刪／誤消費後的定點回補
#
# 什麼時候「不要」用：
#   ✘ TB 級的大 topic：請改用 MirrorMaker 2 跨叢集複寫（scripts/dr/）
#   ✘ 要求嚴格 exactly-once 的還原：匯入是重新 produce，offset 與 timestamp 會變
#
# 已知限制（務必理解後再用）：
#   - 匯入是「重新產生訊息」，新 offset 從目標 topic 目前的 end offset 開始
#   - 訊息 timestamp 會變成匯入當下（除非目標 topic 設 message.timestamp.type=CreateTime
#     且用支援帶入 timestamp 的 producer）
#   - headers 不會保留（console producer 不支援）
#   - partition 落點靠 key 雜湊；只有在「目標 topic 的 partition 數相同」時才會一致
#   - 本工具是「一行一筆」的文字格式：value 含換行、key 含 Tab、
#     或二進位訊息（Avro/Protobuf）都「無法」用本工具匯出。
#     匯出時偵測到換行會直接失敗（不會靜默截斷）；這類 topic 請改用
#     MirrorMaker 2（scripts/dr/）或 Kafka Connect。
#
# 用法：
#   匯出：./scripts/backup/backup-topic-data.sh export <topic> [--max N] [--out DIR]
#   匯入：./scripts/backup/backup-topic-data.sh import <檔案> --topic <目標topic>
#   列出：./scripts/backup/backup-topic-data.sh list
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

DATA_BACKUP_DIR="${DATA_BACKUP_DIR:-${KAFKA_BACKUP_DIR}/topic-data}"
SEPARATOR=$'\t'

usage() { sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }
[[ $# -eq 0 ]] && usage 1
SUBCMD="$1"; shift

case "${SUBCMD}" in

# -----------------------------------------------------------------------------
export)
  [[ $# -ge 1 ]] || die "用法：export <topic> [--max N] [--out DIR] [--from-timestamp MS]"
  TOPIC="$1"; shift
  MAX_MESSAGES=""
  OUT_DIR="${DATA_BACKUP_DIR}"
  FROM_TS=""
  FORMAT="text"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --max)            MAX_MESSAGES="$2"; shift 2 ;;
      --out)            OUT_DIR="$2"; shift 2 ;;
      --from-timestamp) FROM_TS="$2"; shift 2 ;;
      --format)         FORMAT="$2"; shift 2 ;;
      *) die "未知選項：$1" ;;
    esac
  done
  # 誠實面對限制：base64 等格式尚未實作，接受參數再輸出 raw text 會造成
  # 「以為備份了二進位資料」的假象，比直接拒絕更危險
  [[ "${FORMAT}" == "text" ]] \
    || die "--format ${FORMAT} 尚未實作（目前僅支援 text）。二進位 topic 請改用 MirrorMaker 2 或 Kafka Connect。"

  cluster_ready || die "叢集無法連線"
  mkdir -p "${OUT_DIR}"

  # 先看看這個 topic 有多大，避免誤匯出 TB 級資料
  END_OFFSETS="$("$(kafka_bin kafka-get-offsets.sh)" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} \
      --topic "${TOPIC}" 2>/dev/null || die "找不到 topic ${TOPIC}")"
  BEGIN_OFFSETS="$("$(kafka_bin kafka-get-offsets.sh)" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} \
      --topic "${TOPIC}" --time earliest 2>/dev/null || true)"
  END_TOTAL="$(awk -F: '{s+=$3} END{print s+0}' <<<"${END_OFFSETS}")"
  BEGIN_TOTAL="$(awk -F: '{s+=$3} END{print s+0}' <<<"${BEGIN_OFFSETS}")"
  AVAILABLE=$(( END_TOTAL - BEGIN_TOTAL ))

  section "匯出 ${TOPIC}"
  printf '  可匯出訊息數：%s（earliest=%s, latest=%s）\n' "${AVAILABLE}" "${BEGIN_TOTAL}" "${END_TOTAL}" >&2

  if [[ -z "${MAX_MESSAGES}" ]]; then
    MAX_MESSAGES="${AVAILABLE}"
    if (( AVAILABLE > 1000000 )); then
      log_warn "這個 topic 有 ${AVAILABLE} 筆訊息，用 console consumer 匯出會很慢而且吃記憶體。"
      log_warn "超過百萬筆建議改用 MirrorMaker 2 或 Kafka Connect S3 Sink。"
      confirm "仍要全部匯出嗎？" || die "已取消（可用 --max 限制筆數）"
    fi
  fi
  (( MAX_MESSAGES > 0 )) || die "沒有可匯出的訊息"

  TS="$(timestamp)"
  OUT_FILE="${OUT_DIR}/${TOPIC}-${TS}.tsv"
  META_FILE="${OUT_DIR}/${TOPIC}-${TS}.meta"

  PARTITION_COUNT="$(kafka_topics --describe --topic "${TOPIC}" 2>/dev/null | awk '/^Topic: /{print $6; exit}')"

  cat > "${META_FILE}" <<EOF
topic=${TOPIC}
exported_at=${TS}
source_bootstrap=${BOOTSTRAP_SERVERS}
partition_count=${PARTITION_COUNT}
max_messages=${MAX_MESSAGES}
available_messages=${AVAILABLE}
format=${FORMAT}
separator=TAB
columns=partition,offset,timestamp,key,value
EOF

  log_info "開始匯出（最多 ${MAX_MESSAGES} 筆）"

  CONSUMER_ARGS=(
    --bootstrap-server "${BOOTSTRAP_SERVERS}"
    --topic "${TOPIC}"
    --max-messages "${MAX_MESSAGES}"
    --timeout-ms "${EXPORT_TIMEOUT_MS:-60000}"
    --property print.partition=true
    --property print.offset=true
    --property print.timestamp=true
    --property print.key=true
    --property print.value=true
    --property key.separator="${SEPARATOR}"
    --property null.literal=__NULL__
  )
  [[ -n "${KAFKA_CLIENT_CONFIG}" ]] && CONSUMER_ARGS+=(--consumer.config "${KAFKA_CLIENT_CONFIG}")
  if [[ -n "${FROM_TS}" ]]; then
    log_info "從 timestamp ${FROM_TS} 開始"
    # 先用 get-offsets 找出對應 offset，再用 --partition/--offset 消費
    OFFS="$("$(kafka_bin kafka-get-offsets.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
              --topic "${TOPIC}" --time "${FROM_TS}" 2>/dev/null)"
    printf '%s\n' "${OFFS}" | sed 's/^/    /' >&2
    log_warn "--from-timestamp 目前僅供參考，實際匯出仍從 earliest 開始；請用 --max 控制範圍"
    CONSUMER_ARGS+=(--from-beginning)
  else
    CONSUMER_ARGS+=(--from-beginning)
  fi

  "$(kafka_bin kafka-console-consumer.sh)" "${CONSUMER_ARGS[@]}" 2>/dev/null > "${OUT_FILE}.raw" || true

  # 原始格式： CreateTime:<ts>\tPartition:<p>\tOffset:<o>\t<key>\t<value>
  # 正規化成： <partition>\t<offset>\t<timestamp>\t<key>\t<value>
  #
  # value 含換行的訊息會在 console consumer 的輸出裡變成「多行」，
  # 續行沒有 ts/partition/offset 前綴（欄位數 < 5）。這種資料一旦匯出
  # 就是靜默截斷——所以這裡改成偵測到就直接失敗，不產出損毀的備份。
  MALFORMED="$(awk -F'\t' 'NF < 5 || $1 !~ /^[A-Za-z]+:-?[0-9]+$/' "${OUT_FILE}.raw" | grep -c . || true)"
  if (( MALFORMED > 0 )); then
    rm -f "${OUT_FILE}.raw" "${OUT_FILE}" "${META_FILE}"
    die "匯出中止：有 ${MALFORMED} 行無法解析（value 含換行或二進位內容）。
  這個 topic 不適合用本工具做行式文字匯出，請改用 MirrorMaker 2（scripts/dr/）。
  未產出任何備份檔，避免留下看似完整、實際截斷的資料。"
  fi
  awk -F'\t' 'NF>=5 {
      ts = $1; sub(/^[A-Za-z]+:/, "", ts)
      p  = $2; sub(/^Partition:/, "", p)
      o  = $3; sub(/^Offset:/, "", o)
      key = $4
      val = $5
      for (i = 6; i <= NF; i++) val = val "\t" $i
      printf "%s\t%s\t%s\t%s\t%s\n", p, o, ts, key, val
  }' "${OUT_FILE}.raw" > "${OUT_FILE}"
  rm -f "${OUT_FILE}.raw"

  EXPORTED="$(grep -c . "${OUT_FILE}" || true)"
  echo "exported_messages=${EXPORTED}" >> "${META_FILE}"

  gzip -f "${OUT_FILE}"
  sha256sum "${OUT_FILE}.gz" > "${OUT_FILE}.gz.sha256"

  log_ok "匯出 ${EXPORTED} 筆 -> ${OUT_FILE}.gz（$(human_bytes "$(stat -c%s "${OUT_FILE}.gz")")）"
  if (( EXPORTED < MAX_MESSAGES )); then
    log_warn "實際匯出 ${EXPORTED} 筆 < 預期 ${MAX_MESSAGES} 筆（可能是消費逾時，可調高 EXPORT_TIMEOUT_MS）"
  fi
  printf '  meta：%s\n' "${META_FILE}" >&2
  ;;

# -----------------------------------------------------------------------------
import)
  [[ $# -ge 1 ]] || die "用法：import <檔案.tsv.gz> --topic <目標topic>"
  IN_FILE="$1"; shift
  TARGET_TOPIC=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --topic) TARGET_TOPIC="$2"; shift 2 ;;
      *) die "未知選項：$1" ;;
    esac
  done
  [[ -f "${IN_FILE}"     ]] || die "找不到檔案 ${IN_FILE}"
  [[ -n "${TARGET_TOPIC}" ]] || die "請用 --topic 指定目標 topic"
  cluster_ready || die "叢集無法連線"

  # 校驗
  if [[ -f "${IN_FILE}.sha256" ]]; then
    (cd "$(dirname "${IN_FILE}")" && sha256sum -c "$(basename "${IN_FILE}").sha256" >/dev/null 2>&1) \
      && log_ok "SHA256 相符" || die "SHA256 不符，檔案已損毀"
  fi

  META="${IN_FILE%.tsv.gz}.meta"
  SRC_PARTITIONS=""
  [[ -f "${META}" ]] && SRC_PARTITIONS="$(awk -F= '/^partition_count=/{print $2}' "${META}")"

  DST_PARTITIONS="$(kafka_topics --describe --topic "${TARGET_TOPIC}" 2>/dev/null | awk '/^Topic: /{print $6; exit}')"
  [[ -n "${DST_PARTITIONS}" ]] || die "目標 topic ${TARGET_TOPIC} 不存在，請先建立"

  section "匯入到 ${TARGET_TOPIC}"
  printf '  來源檔案       : %s\n' "${IN_FILE}" >&2
  printf '  來源 partition : %s\n' "${SRC_PARTITIONS:-未知}" >&2
  printf '  目標 partition : %s\n' "${DST_PARTITIONS}" >&2

  if [[ -n "${SRC_PARTITIONS}" && "${SRC_PARTITIONS}" != "${DST_PARTITIONS}" ]]; then
    log_warn "partition 數不同（${SRC_PARTITIONS} -> ${DST_PARTITIONS}），key 的落點會改變，順序保證會不同。"
  fi

  BEFORE="$("$(kafka_bin kafka-get-offsets.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --topic "${TARGET_TOPIC}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
  LINES="$(zcat "${IN_FILE}" | grep -c . || true)"
  printf '  目標現有訊息   : %s\n' "${BEFORE}" >&2
  printf '  待匯入筆數     : %s\n' "${LINES}" >&2

  confirm "確定匯入嗎？（訊息會以新的 offset 附加到 topic 尾端）" || die "已取消"

  PRODUCER_ARGS=(
    --bootstrap-server "${BOOTSTRAP_SERVERS}"
    --topic "${TARGET_TOPIC}"
    --property parse.key=true
    --property "key.separator=${SEPARATOR}"
    --property null.marker=__NULL__
    --request-required-acks all
  )
  [[ -n "${KAFKA_CLIENT_CONFIG}" ]] && PRODUCER_ARGS+=(--producer.config "${KAFKA_CLIENT_CONFIG}")

  # 只取 key 與 value 兩欄餵給 producer
  zcat "${IN_FILE}" \
    | awk -F'\t' 'NF>=5 { v=$5; for(i=6;i<=NF;i++) v=v "\t" $i; printf "%s\t%s\n", $4, v }' \
    | "$(kafka_bin kafka-console-producer.sh)" "${PRODUCER_ARGS[@]}" 2>/dev/null

  AFTER="$("$(kafka_bin kafka-get-offsets.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --topic "${TARGET_TOPIC}" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
  IMPORTED=$(( AFTER - BEFORE ))

  if (( IMPORTED == LINES )); then
    log_ok "匯入 ${IMPORTED} 筆，與檔案筆數相符"
  else
    log_warn "匯入 ${IMPORTED} 筆，檔案有 ${LINES} 筆（含換行的訊息可能被拆行）"
  fi
  ;;

# -----------------------------------------------------------------------------
list)
  section "已匯出的 topic 資料"
  if [[ ! -d "${DATA_BACKUP_DIR}" ]]; then
    printf '  （%s 尚無匯出檔）\n' "${DATA_BACKUP_DIR}"
    exit 0
  fi
  printf '  %-40s %-12s %-10s %s\n' "檔案" "大小" "筆數" "來源 topic"
  for f in "${DATA_BACKUP_DIR}"/*.tsv.gz; do
    [[ -e "${f}" ]] || continue
    m="${f%.tsv.gz}.meta"
    cnt="$(awk -F= '/^exported_messages=/{print $2}' "${m}" 2>/dev/null || echo '?')"
    tp="$(awk -F= '/^topic=/{print $2}' "${m}" 2>/dev/null || echo '?')"
    printf '  %-40s %-12s %-10s %s\n' "$(basename "${f}")" \
      "$(human_bytes "$(stat -c%s "${f}")")" "${cnt}" "${tp}"
  done
  ;;

-h|--help|help) usage 0 ;;
*) log_error "未知子指令：${SUBCMD}"; usage 1 ;;
esac
