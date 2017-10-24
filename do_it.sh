#!/usr/bin/env bash
set -e

ANSIBLE_HOME=benchmark/ansible

echo "Running benchmark-suite for ${1}"

mvn -DskipTests=true clean package
  
benchmark/ansible/copy_artifacts.sh 

ansible-playbook -v -i ${ANSIBLE_HOME}/inventories/aws-bench/hosts ${ANSIBLE_HOME}/${1}.yml -t destroy
ansible-playbook -v -i ${ANSIBLE_HOME}/inventories/aws-bench/hosts ${ANSIBLE_HOME}/${1}.yml -t deliver,bootstrap,run
