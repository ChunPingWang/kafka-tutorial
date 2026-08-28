#!/usr/bin/env bash
# =============================================================================
# setup-security.sh - TLS + SASL/SCRAM + ACL 的可執行起手式（README 第 28 章）
#
# 子指令：
#   pki --dir DIR [--cn HOST] [--days N]
#       產生自簽 CA、broker keystore/truststore、client truststore（PKCS12）。
#       所有密碼隨機產生並存於 DIR/passwords.env（權限 600）。
#
#   render --dir DIR [--sasl-port N]
#       產生兩份檔案：
#         DIR/server-security.properties  併入 broker server.properties 的安全性片段
#         DIR/client.properties           SASL_SSL client 設定（含 truststore）
#
#   scram-user --user NAME [--password PW]
#       在「執行中的叢集」建立 SCRAM-SHA-256 使用者（走 BOOTSTRAP_SERVERS）。
#       沒給 --password 就隨機產生並印出。
#       ★ 全新 SASL-only 叢集的第一個管理者請改用 format 時建立：
#         kafka-storage.sh format ... --add-scram 'SCRAM-SHA-256=[name=admin,password=...]'
#
#   acl-app --user NAME --topic PREFIX [--group PREFIX]
#       給應用程式帳號最小權限：指定前綴 topic 的讀寫 + 指定前綴 group 的讀取。
#
# 典型流程（詳見 README 28 章，含單機實測記錄）：
#   1. ./setup-security.sh pki --dir ~/kafka/security --cn kafka-1.internal
#   2. ./setup-security.sh render --dir ~/kafka/security
#   3. 把 server-security.properties 併入 server.properties，滾動重啟
#   4. ./setup-security.sh scram-user --user app-orders
#   5. ./setup-security.sh acl-app --user app-orders --topic orders --group orders-svc
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

need_cmd keytool

rand_pw() { head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 20; }

usage() { sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }
[[ $# -ge 1 ]] || usage 1
SUBCMD="$1"; shift

DIR=""; CN="$(hostname -f 2>/dev/null || echo "${HOSTNAME:-localhost}")"; DAYS=825
SASL_PORT=9094; USER_NAME=""; USER_PW=""; TOPIC_PREFIX=""; GROUP_PREFIX=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)       DIR="$2"; shift 2 ;;
    --cn)        CN="$2"; shift 2 ;;
    --days)      DAYS="$2"; shift 2 ;;
    --sasl-port) SASL_PORT="$2"; shift 2 ;;
    --user)      USER_NAME="$2"; shift 2 ;;
    --password)  USER_PW="$2"; shift 2 ;;
    --topic)     TOPIC_PREFIX="$2"; shift 2 ;;
    --group)     GROUP_PREFIX="$2"; shift 2 ;;
    -h|--help)   usage ;;
    *) die "未知選項：$1" ;;
  esac
done

case "${SUBCMD}" in
# -----------------------------------------------------------------------------
pki)
  [[ -n "${DIR}" ]] || die "pki 需要 --dir"
  mkdir -p "${DIR}"; chmod 700 "${DIR}"
  PW_FILE="${DIR}/passwords.env"
  if [[ -f "${PW_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${PW_FILE}"
    log_info "沿用既有的 ${PW_FILE}"
  else
    CA_PW="$(rand_pw)"; KS_PW="$(rand_pw)"; TS_PW="$(rand_pw)"
    printf 'CA_PW=%s\nKS_PW=%s\nTS_PW=%s\n' "${CA_PW}" "${KS_PW}" "${TS_PW}" > "${PW_FILE}"
    chmod 600 "${PW_FILE}"
  fi

  section "1/4 自簽 CA"
  if [[ -f "${DIR}/ca.crt" ]]; then
    log_info "CA 已存在，略過"
  else
    keytool -genkeypair -alias ca -keyalg RSA -keysize 4096 -validity "${DAYS}" \
      -dname "CN=kafka-lab-ca" -ext bc:c \
      -keystore "${DIR}/ca.p12" -storetype PKCS12 -storepass "${CA_PW}" >/dev/null 2>&1
    keytool -exportcert -alias ca -keystore "${DIR}/ca.p12" -storepass "${CA_PW}" \
      -rfc -file "${DIR}/ca.crt" >/dev/null 2>&1
    log_ok "CA：${DIR}/ca.crt"
  fi

  section "2/4 broker keystore（CN=${CN}）"
  keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 -validity "${DAYS}" \
    -dname "CN=${CN}" -ext "san=dns:${CN},dns:localhost,ip:127.0.0.1" \
    -keystore "${DIR}/broker.keystore.p12" -storetype PKCS12 -storepass "${KS_PW}" >/dev/null 2>&1
  keytool -certreq -alias broker -keystore "${DIR}/broker.keystore.p12" -storepass "${KS_PW}" \
    -file "${DIR}/broker.csr" >/dev/null 2>&1
  keytool -gencert -alias ca -keystore "${DIR}/ca.p12" -storepass "${CA_PW}" \
    -infile "${DIR}/broker.csr" -outfile "${DIR}/broker.crt" \
    -ext "san=dns:${CN},dns:localhost,ip:127.0.0.1" -validity "${DAYS}" -rfc >/dev/null 2>&1
  keytool -importcert -alias ca -noprompt -keystore "${DIR}/broker.keystore.p12" \
    -storepass "${KS_PW}" -file "${DIR}/ca.crt" >/dev/null 2>&1
  keytool -importcert -alias broker -noprompt -keystore "${DIR}/broker.keystore.p12" \
    -storepass "${KS_PW}" -file "${DIR}/broker.crt" >/dev/null 2>&1
  rm -f "${DIR}/broker.csr"
  log_ok "broker keystore：${DIR}/broker.keystore.p12"

  section "3/4 truststore（broker 與 client 共用 CA）"
  for ts in broker.truststore client.truststore; do
    rm -f "${DIR}/${ts}.p12"
    keytool -importcert -alias ca -noprompt -keystore "${DIR}/${ts}.p12" \
      -storetype PKCS12 -storepass "${TS_PW}" -file "${DIR}/ca.crt" >/dev/null 2>&1
  done
  log_ok "truststore：${DIR}/{broker,client}.truststore.p12"

  section "4/4 權限"
  chmod 600 "${DIR}"/*.p12
  log_ok "完成。密碼在 ${PW_FILE}（600）。正式環境請改用公司 CA 簽發，並用密件管理系統存放密碼。"
  ;;

# -----------------------------------------------------------------------------
render)
  [[ -n "${DIR}" && -f "${DIR}/passwords.env" ]] || die "render 需要 --dir（先跑 pki）"
  # shellcheck disable=SC1090
  source "${DIR}/passwords.env"

  cat > "${DIR}/server-security.properties" <<EOF
# ===== 由 setup-security.sh render 產生：併入 server.properties =====
# listener 佈局：PLAINTEXT 留給 broker 間與本機維運（防火牆擋外部），
# 對外只開 SASL_SSL。全面 TLS 時把 inter-broker 也換掉即可。
#   listeners=PLAINTEXT://:9092,CONTROLLER://:9093,SASL_SSL://:${SASL_PORT}
#   advertised.listeners=PLAINTEXT://<內網位址>:9092,SASL_SSL://<對外位址>:${SASL_PORT}
#   listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_SSL:SASL_SSL

ssl.keystore.location=${DIR}/broker.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=${KS_PW}
ssl.truststore.location=${DIR}/broker.truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=${TS_PW}

sasl.enabled.mechanisms=SCRAM-SHA-256
# broker 端的 JAAS 條目：SCRAM 的憑證存在叢集 metadata，這裡只需宣告模組，
# 但「不能省略」——少了它 broker 啟動直接失敗（找不到 KafkaServer JAAS entry）
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;

# ACL：KRaft 用 StandardAuthorizer。super.users 一定要含 broker 自己
# 走的身分（PLAINTEXT listener = User:ANONYMOUS），否則副本同步會被拒。
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:ANONYMOUS
allow.everyone.if.no.acl.found=false
EOF
  chmod 600 "${DIR}/server-security.properties"

  cat > "${DIR}/client.properties" <<EOF
# ===== SASL_SSL client 設定（kafka-*.sh --command-config 用）=====
security.protocol=SASL_SSL
ssl.truststore.location=${DIR}/client.truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=${TS_PW}
sasl.mechanism=SCRAM-SHA-256
# 換成實際帳號密碼（scram-user 子指令的輸出）
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\
  username="admin" \\
  password="CHANGE_ME";
EOF
  chmod 600 "${DIR}/client.properties"
  log_ok "已產生 ${DIR}/server-security.properties 與 ${DIR}/client.properties"
  log_warn "client.properties 的密碼是佔位符，請替換後再用"
  ;;

# -----------------------------------------------------------------------------
scram-user)
  [[ -n "${USER_NAME}" ]] || die "scram-user 需要 --user"
  [[ -n "${USER_PW}" ]] || { USER_PW="$(rand_pw)"; log_info "已隨機產生密碼"; }
  kafka_configs --alter --entity-type users --entity-name "${USER_NAME}" \
    --add-config "SCRAM-SHA-256=[iterations=8192,password=${USER_PW}]"
  log_ok "使用者 ${USER_NAME} 已建立（SCRAM-SHA-256）"
  printf '  username=%s\n  password=%s\n' "${USER_NAME}" "${USER_PW}" >&2
  log_warn "請立刻把密碼存進密件管理系統；這是唯一一次顯示"
  ;;

# -----------------------------------------------------------------------------
acl-app)
  [[ -n "${USER_NAME}" && -n "${TOPIC_PREFIX}" ]] || die "acl-app 需要 --user 與 --topic"
  kafka_acls --add --allow-principal "User:${USER_NAME}" \
    --operation Read --operation Write --operation Describe \
    --topic "${TOPIC_PREFIX}" --resource-pattern-type prefixed
  if [[ -n "${GROUP_PREFIX}" ]]; then
    kafka_acls --add --allow-principal "User:${USER_NAME}" \
      --operation Read --group "${GROUP_PREFIX}" --resource-pattern-type prefixed
  fi
  log_ok "已授權 User:${USER_NAME} 讀寫 topic 前綴「${TOPIC_PREFIX}」$( [[ -n "${GROUP_PREFIX}" ]] && echo "、消費 group 前綴「${GROUP_PREFIX}」" )"
  ;;

-h|--help|help) usage ;;
*) log_error "未知子指令：${SUBCMD}"; usage 1 ;;
esac
