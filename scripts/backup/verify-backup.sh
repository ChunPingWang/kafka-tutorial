#!/usr/bin/env bash
# =============================================================================
# verify-backup.sh - 驗證備份的完整性與可用性
#
# 「沒有驗證過的備份等於沒有備份」。這支腳本做三個層次的驗證：
#   L1 結構    ：必要檔案是否齊全、manifest 是否合理
#   L2 完整性  ：SHA256 校驗碼是否相符
#   L3 可還原  ：把 recreate-topics.sh 對「另一座測試叢集」實跑一遍（--deep）
#
# 用法：
#   ./scripts/backup/verify-backup.sh <備份目錄或 .tar.gz>
#   ./scripts/backup/verify-backup.sh <備份> --deep --target localhost:19092
#   ./scripts/backup/verify-backup.sh --latest
#
# 退出碼：0 = 通過，1 = 失敗
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

TARGET_PATH=""
DEEP=false
DEEP_TARGET=""
TMP_EXTRACT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deep)    DEEP=true; shift ;;
    --target)  DEEP_TARGET="$2"; shift 2 ;;
    --latest)
      TARGET_PATH="$(find "${KAFKA_BACKUP_DIR}" -maxdepth 1 -type d -name '2*Z' 2>/dev/null | sort | tail -1)"
      [[ -n "${TARGET_PATH}" ]] || die "在 ${KAFKA_BACKUP_DIR} 找不到任何備份"
      shift ;;
    -h|--help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) TARGET_PATH="$1"; shift ;;
  esac
done

[[ -n "${TARGET_PATH}" ]] || die "請指定備份路徑，或使用 --latest"

# 注意：EXIT trap 在 set -e 下若以非零狀態結束，會蓋掉腳本原本的退出碼，故補 return 0
cleanup() { [[ -n "${TMP_EXTRACT}" ]] && rm -rf "${TMP_EXTRACT}"; return 0; }
trap cleanup EXIT

# 若給的是壓縮檔，先解開
if [[ -f "${TARGET_PATH}" && "${TARGET_PATH}" == *.tar.gz ]]; then
  section "解開壓縮檔"
  if [[ -f "${TARGET_PATH}.sha256" ]]; then
    if (cd "$(dirname "${TARGET_PATH}")" && sha256sum -c "$(basename "${TARGET_PATH}").sha256" >/dev/null 2>&1); then
      log_ok "壓縮檔 SHA256 相符"
    else
      die "壓縮檔 SHA256 不符，備份已損毀"
    fi
  else
    log_warn "找不到 .sha256，跳過壓縮檔校驗"
  fi
  TMP_EXTRACT="$(mktemp -d)"
  tar -xzf "${TARGET_PATH}" -C "${TMP_EXTRACT}"
  TARGET_PATH="$(find "${TMP_EXTRACT}" -maxdepth 1 -type d -name '2*Z' | head -1)"
  [[ -n "${TARGET_PATH}" ]] || die "壓縮檔結構不符預期"
fi

[[ -d "${TARGET_PATH}" ]] || die "找不到備份目錄：${TARGET_PATH}"

PASS=0; FAIL=0
ok() { PASS=$(( PASS + 1 )); printf '  %s✔%s %s\n' "${C_GRN}" "${C_RESET}" "$*"; }
ng() { FAIL=$(( FAIL + 1 )); printf '  %s✘%s %s\n' "${C_RED}" "${C_RESET}" "$*"; }

# -----------------------------------------------------------------------------
section "L1 結構檢查：${TARGET_PATH}"
for f in manifest.txt topics/topic-list.txt topics/recreate-topics.sh \
         groups/group-list.txt config/server.properties cluster/cluster-id.txt; do
  if [[ -f "${TARGET_PATH}/${f}" ]]; then
    ok "${f}"
  else
    ng "缺少 ${f}"
  fi
done

if [[ -f "${TARGET_PATH}/manifest.txt" ]]; then
  # shellcheck disable=SC2046
  eval $(grep -E '^(backup_id|cluster_id|topic_count|group_count|broker_count|kafka_version)=' "${TARGET_PATH}/manifest.txt" | sed 's/^/M_/')
  printf '\n'
  printf '  backup_id     : %s\n' "${M_backup_id:-?}"
  printf '  cluster_id    : %s\n' "${M_cluster_id:-?}"
  printf '  kafka_version : %s\n' "${M_kafka_version:-?}"
  printf '  brokers/topics/groups : %s / %s / %s\n\n' \
    "${M_broker_count:-?}" "${M_topic_count:-?}" "${M_group_count:-?}"

  if [[ "${M_cluster_id:-unknown}" == "unknown" || -z "${M_cluster_id:-}" ]]; then
    ng "manifest 沒有記錄 cluster_id"
  else
    ok "manifest 記錄了 cluster_id"
  fi

  # topic 數量對得起來嗎
  ACTUAL_TOPICS="$(grep -vc '^__' "${TARGET_PATH}/topics/topic-list.txt" 2>/dev/null || true)"
  if [[ "${ACTUAL_TOPICS}" == "${M_topic_count:-}" ]]; then
    ok "topic 數量與 manifest 一致（${ACTUAL_TOPICS}）"
  else
    ng "topic 數量不一致：清單 ${ACTUAL_TOPICS} vs manifest ${M_topic_count:-?}"
  fi
fi

# recreate 腳本語法是否正確（壞掉的還原腳本比沒有更糟）
if bash -n "${TARGET_PATH}/topics/recreate-topics.sh" 2>/dev/null; then
  ok "recreate-topics.sh 語法正確"
else
  ng "recreate-topics.sh 語法錯誤"
fi
if [[ -f "${TARGET_PATH}/groups/restore-offsets.sh" ]]; then
  if bash -n "${TARGET_PATH}/groups/restore-offsets.sh" 2>/dev/null; then
    ok "restore-offsets.sh 語法正確"
  else
    ng "restore-offsets.sh 語法錯誤"
  fi
fi

# -----------------------------------------------------------------------------
section "L2 完整性檢查"
if [[ -f "${TARGET_PATH}/SHA256SUMS" ]]; then
  if (cd "${TARGET_PATH}" && sha256sum -c SHA256SUMS >/dev/null 2>&1); then
    ok "所有檔案 SHA256 相符（$(grep -c . "${TARGET_PATH}/SHA256SUMS") 個檔案）"
  else
    BAD="$( (cd "${TARGET_PATH}" && sha256sum -c SHA256SUMS 2>/dev/null) | grep -v ': OK$' | head -5 || true)"
    ng "校驗失敗的檔案："
    printf '%s\n' "${BAD}" | sed 's/^/      /'
  fi
else
  ng "缺少 SHA256SUMS"
fi

# -----------------------------------------------------------------------------
section "L3 可還原性檢查"
if [[ "${DEEP}" != "true" ]]; then
  printf '  %s略過%s（加上 --deep --target <測試叢集> 才會實際還原驗證）\n' "${C_DIM}" "${C_RESET}"
  printf '  %s強烈建議每月至少做一次 --deep 驗證，並記錄還原耗時作為 RTO 依據。%s\n' "${C_DIM}" "${C_RESET}"
else
  [[ -n "${DEEP_TARGET}" ]] || die "--deep 必須搭配 --target <bootstrap>"
  if [[ "${DEEP_TARGET}" == "${BOOTSTRAP_SERVERS}" ]]; then
    die "--target 不可指向來源叢集 ${BOOTSTRAP_SERVERS}，請用獨立的測試叢集"
  fi

  log_info "對 ${DEEP_TARGET} 執行 recreate-topics.sh"
  START_TS="$(date +%s)"
  if BOOTSTRAP_SERVERS="${DEEP_TARGET}" KAFKA_HOME="${KAFKA_HOME}" \
       bash "${TARGET_PATH}/topics/recreate-topics.sh" >/dev/null 2>&1; then
    ok "topic 重建成功"
  else
    ng "topic 重建失敗"
  fi

  # 逐一比對還原後的 topic 是否與備份一致
  MISMATCH=0
  while IFS= read -r t; do
    [[ -z "${t}" || "${t}" == __* ]] && continue
    EXPECT_P="$(awk '/^Topic: /{print $6; exit}' "${TARGET_PATH}/topics/${t//\//_}.describe" 2>/dev/null || echo "")"
    ACTUAL_P="$("${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${DEEP_TARGET}" \
                  --describe --topic "${t}" 2>/dev/null | awk '/^Topic: /{print $6; exit}')"
    if [[ "${EXPECT_P}" != "${ACTUAL_P}" ]]; then
      ng "${t} partition 數不符：預期 ${EXPECT_P}，實際 ${ACTUAL_P:-<不存在>}"
      MISMATCH=$(( MISMATCH + 1 ))
    fi
  done < "${TARGET_PATH}/topics/topic-list.txt"
  (( MISMATCH == 0 )) && ok "所有 topic 的 partition 數與備份一致"

  ELAPSED=$(( $(date +%s) - START_TS ))
  printf '\n  還原耗時：%d 秒（可作為 RTO 估算的基準）\n' "${ELAPSED}"
fi

# -----------------------------------------------------------------------------
section "驗證結果"
printf '  通過：%d  失敗：%d\n' "${PASS}" "${FAIL}"
if (( FAIL > 0 )); then
  log_error "備份驗證失敗，請重新備份"
  exit 1
fi
log_ok "備份驗證通過"
