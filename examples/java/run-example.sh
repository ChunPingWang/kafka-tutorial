#!/usr/bin/env bash
# =============================================================================
# run-example.sh - 編譯並執行本目錄的 Java 範例（零外部依賴）
#
# classpath 直接用 Kafka 發行版自帶的 ${KAFKA_HOME}/libs/*，
# 不需要 Maven / Gradle——裝好 Kafka 就能跑。
#
# 用法：
#   ./run-example.sh TransactionalProducer [topic]
#   ./run-example.sh DlqRetryConsumer [topic]
#   ./run-example.sh WordCountStream
#
# 環境變數：
#   BOOTSTRAP_SERVERS  目標叢集（預設 localhost:9092）
#   KAFKA_HOME         Kafka 安裝目錄（預設 ~/kafka/current）
#   RUN_FOR_MS         consumer/streams 範例的執行時長（預設 30000）
# =============================================================================
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
SELF="$(pwd)/$(basename "${BASH_SOURCE[0]}")"

KAFKA_HOME="${KAFKA_HOME:-${HOME}/kafka/current}"
[[ -d "${KAFKA_HOME}/libs" ]] || { echo "找不到 ${KAFKA_HOME}/libs，請先安裝 Kafka 或設 KAFKA_HOME" >&2; exit 1; }
command -v javac >/dev/null || { echo "需要 JDK（javac）" >&2; exit 1; }

[[ $# -ge 1 && "$1" != "-h" && "$1" != "--help" ]] || { sed -n '2,17p' "${SELF}" | sed 's/^# \{0,1\}//'; exit 0; }
CLASS="$1"; shift
[[ -f "${CLASS}.java" ]] || { echo "找不到 ${CLASS}.java" >&2; exit 1; }

CP="${KAFKA_HOME}/libs/*"
mkdir -p build
javac -cp "${CP}" -d build "${CLASS}.java"
exec java -cp "build:${CP}" "${CLASS}" "$@"
