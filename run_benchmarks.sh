#!/usr/bin/env bash
readonly ANSIBLE_HOME=benchmark/ansible
readonly DOCKER_COMPOSE=benchmark/ansible/docker-compose.yml

package() {
  mvn -DskipTests=true clean package
}

copy_worker_artifacts() {
  local worker=runtime/target/flamestream-runtime-1.0-SNAPSHOT.jar
  local dependencies=runtime/target/lib
  local entrypoint=runtime/target/entrypoint.sh
  local files="${ANSIBLE_HOME}/roles/flamestream-worker/files/"

  if [[ -f "$worker" && -d "$dependencies" && -f "$entrypoint" ]]; then
    cp "$worker" "$files"
    cp "$entrypoint" "$files"
    cp -r "$dependencies" "$files"
  else
    echo "Some artifacts hasn't been found";
  fi
}

docker_compose_up() {
  sudo docker-compose -f "$DOCKER_COMPOSE" up -d --scale worker=3
}

deploy() {
  sudo ansible-playbook -v -i "${ANSIBLE_HOME}/local.yml" "${ANSIBLE_HOME}/flamestream.yml"
}

local_bench() {
  docker_compose_up
  package
  copy_worker_artifacts
  deploy
}

[[ "$0" == "$BASH_SOURCE" ]] && eval "$@"
