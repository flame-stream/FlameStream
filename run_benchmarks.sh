#!/usr/bin/env bash
readonly ANSIBLE_HOME=benchmark/ansible
readonly DOCKER_COMPOSE=benchmark/ansible/docker-compose.yml

package() {
  mvn -DskipTests=true clean package
}

copy_worker_artifacts() {
  local files="${ANSIBLE_HOME}/roles/flamestream-common/files/"
  local worker=runtime/target/flamestream-runtime-1.0-SNAPSHOT.jar
  local dependencies=runtime/target/lib
  local entrypoint=runtime/target/entrypoint.sh
  local examples=examples/target/flamestream-examples-1.0-SNAPSHOT.jar
  local examples_dependencies=examples/target/lib

  if [[ -f "$worker" && -d "$dependencies" && -f "$entrypoint" && -f "$examples" && -d "$examples_dependencies" ]]; then
    cp "$worker" "$files"
    cp "$entrypoint" "$files"
    cp -r "$dependencies" "$files"
    cp -rp "$examples_dependencies" "$files"
    cp -rp "$examples" "${files}/lib"
  else
    echo "Some artifacts hasn't been found"
    return 1
  fi
}

docker_compose_up() {
  sudo docker-compose -f "$DOCKER_COMPOSE" up -d
}

deploy() {
  sudo ansible-playbook -v -i "${ANSIBLE_HOME}/local.yml" "${ANSIBLE_HOME}/flamestream.yml"
}

local_bench() {
  docker_compose_up \
    && package \
    && copy_worker_artifacts \
    && deploy
}

[[ "$0" == "$BASH_SOURCE" ]] && local_bench
