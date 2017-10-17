#!/usr/bin/env bash
set -e

ANSIBLE_HOME=benchmark/ansible

echo "Running benchmark-suite for ${1}"

mvn clean 
mvn -DskipTests=true package
  
benchmark/ansible/copy_artifacts.sh 

ansible-playbook -v -i ${ANSIBLE_HOME}/inventories/aws/hosts ${ANSIBLE_HOME}/${1}.yml -t destroy
ansible-playbook -v -i ${ANSIBLE_HOME}/inventories/aws/hosts ${ANSIBLE_HOME}/${1}.yml -t deliver,bootstrap,run
