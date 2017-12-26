#!/usr/bin/env bash
readonly ANSIBLE_HOME=benchmark/ansible
readonly DOCKER_COMPOSE=benchmark/ansible/docker-compose.yml

package() {
  mvn -DskipTests=true clean package
}

copy_artifacts() {
  local worker=runtime/target/flamestream-runtime-1.0-SNAPSHOT-uber.jar
  local worker_files="${ANSIBLE_HOME}/roles/flamestream-worker/files/"

  local bench=benchmark/bench_stand/target/flamestream-benchmark-1.0-SNAPSHOT-uber.jar
  local bench_files="${ANSIBLE_HOME}/roles/flamestream-bench/files/"

  local fs_graph=examples/target/flamestream-examples-1.0-SNAPSHOT.jar
  local fs_graph_files="${ANSIBLE_HOME}benchmark/ansible/roles/flink-bench/files/"

  local flink_graph=examples/target/flamestream-examples-1.0-SNAPSHOT.jar
  local flink_graph_files="${ANSIBLE_HOME}benchmark/ansible/roles/flamestream-bench/files/"

  if [[ -f ${worker} && -f ${bench} && -f ${fs_graph} && -f ${flink_graph} ]]; then
    cp ${worker} ${worker_files}
    cp ${bench} ${bench_files};
    cp ${flink_graph} ${flink_graph_files};
    cp ${flink_graph} ${fs_graph_files};
  else
    echo "Some jars hasn't been found";
  fi
}

docker_compose_up() {
  sudo docker-compose up -f "$DOCKER_COMPOSE" -d --scale worker=5
}

deploy() {
  ansible-playbook -v -i "${ANSIBLE_HOME}/local.yml" "${ANSIBLE_HOME}/flamestream.yml"
}

local_bench() {
  package
  docker_compose_up
  deploy
}

local_bench
