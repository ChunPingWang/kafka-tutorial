#!/usr/bin/env bash
# =============================================================================
# backup-cluster.sh - 叢集設定與 metadata 備份
#
# 備份什麼（也就是「災難時重建叢集需要什麼」）：
#   1. broker 設定檔（server.properties / log4j / JAAS 檔名清單）
#   2. cluster.id 與 KRaft meta.properties
#   3. 所有 topic 的定義（partition / RF / 副本配置 / 動態設定）
#   4. 所有 consumer group 的 offset
#   5. broker 層級的動態設定（kafka-configs 的 broker entity）
#   6. ACL 清單（若有啟用授權）
#   7. quota 設定
#   8. 叢集拓撲快照（broker 清單、quorum 狀態、log dir 用量）
#
# 不備份什麼：
#   訊息本身。訊息量級通常無法用檔案備份處理，正確做法是「跨叢集複寫」
#   （見 scripts/dr/setup-mirrormaker.sh）。這支腳本備份的是「重建叢集骨架」
#   所需的一切；資料的復原能力來自複寫與副本。
#
# 產出：${KAFKA_BACKUP_DIR}/<timestamp>/ 以及同名 .tar.gz
#
# 用法：
#   ./scripts/backup/backup-cluster.sh
#   ./scripts/backup/backup-cluster.sh --output /mnt/nas/kafka-backups
#   RETENTION_DAYS=30 ./scripts/backup/backup-cluster.sh
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

OUTPUT_BASE="${KAFKA_BACKUP_DIR}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
COMPRESS=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)     OUTPUT_BASE="$2"; shift 2 ;;
    --retention)  RETENTION_DAYS="$2"; shift 2 ;;
    --no-compress) COMPRESS=false; shift ;;
    -h|--help)    sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

TS="$(timestamp)"
BACKUP_DIR="${OUTPUT_BASE}/${TS}"
mkdir -p "${BACKUP_DIR}"/{config,topics,groups,acls,cluster}

cluster_ready || die "叢集無法連線：${BOOTSTRAP_SERVERS}（設定檔備份仍可手動進行）"

section "備份到 ${BACKUP_DIR}"

# -----------------------------------------------------------------------------
# 1. 設定檔
# -----------------------------------------------------------------------------
log_info "[1/8] broker 設定檔"
for f in server.properties cluster-info; do
  [[ -f "${KAFKA_CONF_DIR}/${f}" ]] && cp "${KAFKA_CONF_DIR}/${f}" "${BACKUP_DIR}/config/"
done
if [[ -d "${KAFKA_HOME}/config" ]]; then
  # log4j / tools-log4j 等，只抓純設定不抓 jar
  find "${KAFKA_HOME}/config" -maxdepth 1 -type f \( -name '*.properties' -o -name '*.yaml' \) \
    -exec cp {} "${BACKUP_DIR}/config/" \; 2>/dev/null || true
fi
log_ok "$(find "${BACKUP_DIR}/config" -type f | wc -l) 個設定檔"

# -----------------------------------------------------------------------------
# 2. cluster.id / meta.properties
# -----------------------------------------------------------------------------
log_info "[2/8] KRaft metadata 識別"
if [[ -f "${KAFKA_DATA_DIR}/meta.properties" ]]; then
  cp "${KAFKA_DATA_DIR}/meta.properties" "${BACKUP_DIR}/cluster/"
fi
kafka_cluster cluster-id > "${BACKUP_DIR}/cluster/cluster-id.txt" 2>/dev/null || true
# 後援：直接讀本機 meta.properties
if [[ ! -s "${BACKUP_DIR}/cluster/cluster-id.txt" && -f "${KAFKA_DATA_DIR}/meta.properties" ]]; then
  grep -E '^cluster.id=' "${KAFKA_DATA_DIR}/meta.properties" > "${BACKUP_DIR}/cluster/cluster-id.txt" || true
fi
kafka_metadata describe --status > "${BACKUP_DIR}/cluster/quorum-status.txt" 2>/dev/null || true
kafka_metadata describe --replication > "${BACKUP_DIR}/cluster/quorum-replication.txt" 2>/dev/null || true
CLUSTER_ID_VAL="$(grep -oE '[A-Za-z0-9_-]{22}' "${BACKUP_DIR}/cluster/cluster-id.txt" 2>/dev/null | head -1 || true)"
log_ok "cluster.id：${CLUSTER_ID_VAL:-n/a}"

# -----------------------------------------------------------------------------
# 3. Topic 定義
# -----------------------------------------------------------------------------
log_info "[3/8] topic 定義"
kafka_topics --list 2>/dev/null | sort > "${BACKUP_DIR}/topics/topic-list.txt" || true
kafka_topics --describe 2>/dev/null > "${BACKUP_DIR}/topics/topic-describe.txt" || true

# 產生「可重放」的重建腳本：災難復原時直接執行就能還原所有 topic 定義
RECREATE="${BACKUP_DIR}/topics/recreate-topics.sh"
{
  echo '#!/usr/bin/env bash'
  echo '# 由 backup-cluster.sh 自動產生：重建所有 topic 的定義（不含資料）'
  echo '# 用法：BOOTSTRAP_SERVERS=new-cluster:9092 bash recreate-topics.sh'
  echo '# 單一 topic 失敗（例如目標叢集 broker 數少於 RF）不會中斷其他 topic，'
  echo '# 全部嘗試完之後統一回報失敗清單。'
  echo 'set -Eeuo pipefail'
  echo 'BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"'
  echo 'KAFKA_HOME="${KAFKA_HOME:-'"${KAFKA_HOME}"'}"'
  echo 'T="${KAFKA_HOME}/bin/kafka-topics.sh"'
  echo 'FAILED=0; FAILED_TOPICS=""'
  echo ''
} > "${RECREATE}"

TOPIC_COUNT=0
while IFS= read -r t; do
  [[ -z "${t}" ]] && continue
  # 內部 topic 由 Kafka 自行建立，不需要也不應該手動重建
  [[ "${t}" == __* ]] && continue
  DESC="$(kafka_topics --describe --topic "${t}" 2>/dev/null || true)"
  [[ -z "${DESC}" ]] && continue

  PARTS="$(awk '/^Topic: /{print $6; exit}' <<<"${DESC}")"
  RF="$(awk '/^Topic: /{print $8; exit}' <<<"${DESC}")"
  CFG="$(awk '/^Topic: /{for(i=1;i<=NF;i++) if($i=="Configs:") {print $(i+1); exit}}' <<<"${DESC}")"

  {
    printf 'echo "建立 %s"\n' "${t}"
    printf '"${T}" --bootstrap-server "${BOOTSTRAP_SERVERS}" --create --if-not-exists \\\n'
    printf '  --topic %q --partitions %s --replication-factor %s' "${t}" "${PARTS}" "${RF}"
    if [[ -n "${CFG}" && "${CFG}" != "Configs:" ]]; then
      # Configs 以逗號分隔，逐一轉成 --config
      IFS=',' read -ra KV <<<"${CFG}"
      for kv in "${KV[@]}"; do
        [[ -z "${kv}" ]] && continue
        printf ' \\\n  --config %q' "${kv}"
      done
    fi
    printf ' \\\n  || { echo "  ✘ 建立失敗：%q" >&2; FAILED=$((FAILED+1)); FAILED_TOPICS="${FAILED_TOPICS} %q"; }\n\n' "${t}" "${t}"
  } >> "${RECREATE}"

  # 完整副本配置（哪個 partition 放在哪些 broker）另存，供需要保持配置一致時使用
  kafka_topics --describe --topic "${t}" 2>/dev/null > "${BACKUP_DIR}/topics/${t//\//_}.describe" || true
  TOPIC_COUNT=$(( TOPIC_COUNT + 1 ))
done < "${BACKUP_DIR}/topics/topic-list.txt"

{
  echo 'if (( FAILED > 0 )); then'
  echo '  echo "" >&2'
  echo '  echo "⚠ ${FAILED} 個 topic 建立失敗（其餘皆已嘗試）：${FAILED_TOPICS# }" >&2'
  echo '  echo "  常見原因：目標叢集 broker 數少於備份來源的 replication factor。" >&2'
  echo '  echo "  可用較低 RF 手動重建這些 topic，或先擴充目標叢集。" >&2'
  echo '  exit 1'
  echo 'fi'
  echo 'echo "所有 topic 建立完成"'
} >> "${RECREATE}"

chmod +x "${RECREATE}"
log_ok "${TOPIC_COUNT} 個使用者 topic，已產生 recreate-topics.sh"

# -----------------------------------------------------------------------------
# 4. Consumer group offsets
# -----------------------------------------------------------------------------
log_info "[4/8] consumer group offsets"
kafka_groups --list 2>/dev/null | sort > "${BACKUP_DIR}/groups/group-list.txt" || true

RESET="${BACKUP_DIR}/groups/restore-offsets.sh"
{
  echo '#!/usr/bin/env bash'
  echo '# 由 backup-cluster.sh 自動產生：把 consumer group offset 還原到備份當下的位置'
  echo '# 前提：目標叢集已有相同的 topic，且該 group 的 consumer 全部停止'
  echo '# 用法：BOOTSTRAP_SERVERS=new-cluster:9092 bash restore-offsets.sh'
  echo '# 單一 group 失敗不會中斷其他 group，全部嘗試完之後統一回報。'
  echo 'set -Eeuo pipefail'
  echo 'BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"'
  echo 'KAFKA_HOME="${KAFKA_HOME:-'"${KAFKA_HOME}"'}"'
  echo 'G="${KAFKA_HOME}/bin/kafka-consumer-groups.sh"'
  echo 'FAILED=0'
  echo ''
} > "${RESET}"

GROUP_TOTAL=0
while IFS= read -r g; do
  [[ -z "${g}" ]] && continue
  OUT="$(kafka_groups --describe --group "${g}" 2>/dev/null || true)"
  printf '%s\n' "${OUT}" > "${BACKUP_DIR}/groups/${g//\//_}.describe"

  # 產生 CSV（kafka-consumer-groups --reset-offsets --from-file 可吃的格式）
  CSV="${BACKUP_DIR}/groups/${g//\//_}.offsets.csv"
  printf '%s\n' "${OUT}" | awk -v g="${g}" '
    $1==g && $3 ~ /^[0-9]+$/ && $4 ~ /^[0-9]+$/ { printf "%s,%s,%s\n", $2, $3, $4 }' > "${CSV}"

  if [[ -s "${CSV}" ]]; then
    {
      printf 'echo "還原 group %s"\n' "${g}"
      printf '"${G}" --bootstrap-server "${BOOTSTRAP_SERVERS}" --group %q \\\n' "${g}"
      printf '  --reset-offsets --from-file "$(dirname "$0")/%s" --execute \\\n' "$(basename "${CSV}")"
      printf '  || { echo "  ✘ 還原失敗：%q（topic 不存在或 group 仍有 active consumer？）" >&2; FAILED=$((FAILED+1)); }\n\n' "${g}"
    } >> "${RESET}"
    GROUP_TOTAL=$(( GROUP_TOTAL + 1 ))
  fi
done < "${BACKUP_DIR}/groups/group-list.txt"

{
  echo 'if (( FAILED > 0 )); then'
  echo '  echo "⚠ ${FAILED} 個 group 還原失敗（其餘皆已嘗試）" >&2'
  echo '  exit 1'
  echo 'fi'
  echo 'echo "所有 group offset 還原完成"'
} >> "${RESET}"

chmod +x "${RESET}"
log_ok "${GROUP_TOTAL} 個 group 的 offset"

# -----------------------------------------------------------------------------
# 5. Broker 動態設定
# -----------------------------------------------------------------------------
log_info "[5/8] broker 動態設定"
kafka_configs --entity-type brokers --describe --all \
  > "${BACKUP_DIR}/config/broker-dynamic-configs.txt" 2>/dev/null || true
kafka_configs --entity-type brokers --entity-default --describe \
  > "${BACKUP_DIR}/config/broker-default-configs.txt" 2>/dev/null || true
log_ok "已存"

# -----------------------------------------------------------------------------
# 6. ACL
# -----------------------------------------------------------------------------
log_info "[6/8] ACL"
if kafka_acls --list > "${BACKUP_DIR}/acls/acls.txt" 2>/dev/null; then
  ACL_COUNT="$(grep -c 'principal' "${BACKUP_DIR}/acls/acls.txt" 2>/dev/null || true)"
  log_ok "${ACL_COUNT} 條 ACL"
else
  echo "叢集未啟用 authorizer，或目前使用者無權限查詢" > "${BACKUP_DIR}/acls/acls.txt"
  log_info "未啟用 ACL（略過）"
fi

# -----------------------------------------------------------------------------
# 7. Quota
# -----------------------------------------------------------------------------
log_info "[7/8] quota 設定"
for et in users clients topics; do
  kafka_configs --entity-type "${et}" --describe \
    > "${BACKUP_DIR}/config/quota-${et}.txt" 2>/dev/null || true
done
log_ok "已存"

# -----------------------------------------------------------------------------
# 8. 叢集拓撲快照與 manifest
# -----------------------------------------------------------------------------
log_info "[8/8] 叢集快照與 manifest"
"$(kafka_bin kafka-broker-api-versions.sh)" --bootstrap-server "${BOOTSTRAP_SERVERS}" 2>/dev/null \
  | grep -E '^\S+:[0-9]+ \(id:' > "${BACKUP_DIR}/cluster/brokers.txt" || true
kafka_logdirs --describe > "${BACKUP_DIR}/cluster/log-dirs.json" 2>/dev/null || true

cat > "${BACKUP_DIR}/manifest.txt" <<EOF
# Kafka 叢集備份 manifest
backup_id=${TS}
created_at=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
created_by=$(id -un)@$(hostname -f 2>/dev/null || hostname)
bootstrap_servers=${BOOTSTRAP_SERVERS}
kafka_version=${KAFKA_VERSION}
cluster_id=${CLUSTER_ID_VAL:-unknown}
broker_count=$(grep -c 'id:' "${BACKUP_DIR}/cluster/brokers.txt" 2>/dev/null || true)
topic_count=${TOPIC_COUNT}
group_count=${GROUP_TOTAL}
EOF

# 校驗碼：還原前可用 verify-backup.sh 確認檔案未損毀
( cd "${BACKUP_DIR}" && find . -type f ! -name 'SHA256SUMS' -print0 \
    | sort -z | xargs -0 sha256sum > SHA256SUMS ) 2>/dev/null || true

log_ok "manifest 與校驗碼已產生"

# -----------------------------------------------------------------------------
# 打包
# -----------------------------------------------------------------------------
ARCHIVE=""
if [[ "${COMPRESS}" == "true" ]]; then
  section "打包"
  ARCHIVE="${OUTPUT_BASE}/kafka-backup-${TS}.tar.gz"
  tar -czf "${ARCHIVE}" -C "${OUTPUT_BASE}" "${TS}"
  sha256sum "${ARCHIVE}" > "${ARCHIVE}.sha256"
  log_ok "${ARCHIVE}（$(human_bytes "$(stat -c%s "${ARCHIVE}" 2>/dev/null || stat -f%z "${ARCHIVE}")")）"
fi

# -----------------------------------------------------------------------------
# 同步到遠端（選用）
# -----------------------------------------------------------------------------
if [[ -n "${BACKUP_REMOTE_URI:-}" && -n "${ARCHIVE}" ]]; then
  section "上傳到 ${BACKUP_REMOTE_URI}"
  case "${BACKUP_REMOTE_URI}" in
    s3://*)  command -v aws >/dev/null   && run aws s3 cp "${ARCHIVE}" "${BACKUP_REMOTE_URI}/"     || log_warn "找不到 aws cli，略過上傳" ;;
    gs://*)  command -v gsutil >/dev/null && run gsutil cp "${ARCHIVE}" "${BACKUP_REMOTE_URI}/"    || log_warn "找不到 gsutil，略過上傳" ;;
    az://*)  command -v az >/dev/null    && run az storage blob upload -f "${ARCHIVE}" --container-name "${BACKUP_REMOTE_URI#az://}" || log_warn "找不到 az cli，略過上傳" ;;
    /*)      run cp "${ARCHIVE}" "${BACKUP_REMOTE_URI}/" ;;
    *)       log_warn "不支援的 BACKUP_REMOTE_URI：${BACKUP_REMOTE_URI}" ;;
  esac
fi

# -----------------------------------------------------------------------------
# 清理過期備份
# -----------------------------------------------------------------------------
section "清理超過 ${RETENTION_DAYS} 天的備份"
DELETED=0
while IFS= read -r -d '' old; do
  log_info "刪除 ${old}"
  rm -rf "${old}"
  DELETED=$(( DELETED + 1 ))
done < <(find "${OUTPUT_BASE}" -maxdepth 1 -mtime "+${RETENTION_DAYS}" \
           \( -name '2*Z' -o -name 'kafka-backup-*.tar.gz*' \) -print0 2>/dev/null)
log_ok "刪除 ${DELETED} 份過期備份"

# -----------------------------------------------------------------------------
section "備份完成"
cat >&2 <<EOF
  目錄     : ${BACKUP_DIR}
  壓縮檔   : ${ARCHIVE:-（未壓縮）}
  topic    : ${TOPIC_COUNT}
  group    : ${GROUP_TOTAL}

  下一步（強烈建議）：
    ./scripts/backup/verify-backup.sh ${BACKUP_DIR}
  沒有驗證過的備份，等於沒有備份。
EOF
