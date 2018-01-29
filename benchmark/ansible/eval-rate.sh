#!/usr/bin/env bash

run() {
  for rate in 300 200 150 100 80 40 20 10; do
    ansible-playbook -v -i aws.yml flamestream.yml --extra-vars "rate=$rate"
    echo "$rate done"
  done;

  echo "Win"
}

run
