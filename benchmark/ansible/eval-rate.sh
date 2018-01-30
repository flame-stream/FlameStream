#!/usr/bin/env bash

run() {
  for rate in 300 250 200 150 125 100 90 80 70; do
    ansible-playbook -v -i aws.yml flamestream.yml --extra-vars "rate=$rate"
    echo "$rate done"
  done;

  echo "Win"
}

run
