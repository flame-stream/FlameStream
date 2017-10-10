#!/usr/bin/env bash

PROJECT_ROOT='../../'

FLAMESTREAM_WORKER="${PROJECT_ROOT}runtime/target/flamestream-runtime-1.0-SNAPSHOT-uber.jar"
FLAMESTREAM_BENCH="${PROJECT_ROOT}benchmark/flamestream-benchmark/target/flamestream-benchmark-1.0-SNAPSHOT-uber.jar"
FLINK_BENCH="${PROJECT_ROOT}benchmark/flink-benchmark/target/flink-benchmark-1.0-SNAPSHOT-uber.jar"

BENCHMARK_BUSINESS="${PROJECT_ROOT}examples/target/flamestream-examples-1.0-SNAPSHOT.jar"

ANSIBLE_WORKER_FILES="${PROJECT_ROOT}benchmark/ansible/roles/flamestream-worker/files/"
ANSIBLE_FLAMESTREAM_BENCH_FILES="${PROJECT_ROOT}benchmark/ansible/roles/flamestream-bench/files/"
ANSIBLE_FLINK_BENCH_FILES="${PROJECT_ROOT}benchmark/ansible/roles/flink-bench/files/"

if [[ -f ${FLAMESTREAM_WORKER} && -f ${BENCHMARK_BUSINESS} && -f ${FLAMESTREAM_BENCH} && -f ${FLINK_BENCH} ]]; then
  /bin/cp ${FLAMESTREAM_WORKER} ${BENCHMARK_BUSINESS} ${ANSIBLE_WORKER_FILES};
  /bin/cp ${FLAMESTREAM_BENCH} ${ANSIBLE_FLAMESTREAM_BENCH_FILES};
  /bin/cp ${FLINK_BENCH} ${ANSIBLE_FLINK_BENCH_FILES};

else
  echo "jars hasn't been found";
fi

