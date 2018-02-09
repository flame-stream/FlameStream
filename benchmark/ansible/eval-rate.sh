#!/usr/bin/env bash

main() {
  for rate in 110 100 90 80 70 60 50 40; do
    ansible-playbook -v -i aws.yml flamestream.yml --extra-vars "rate=$rate"
    echo "$rate done"
  done;

  echo "Win"
}

main
