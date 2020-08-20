#!/bin/bash

set -o errexit -o nounset -o pipefail


function write_config_options(){
    echo "write config options to $1"
    eval "echo \"$(cat $1)\"" > $1
}

function main(){
  write_config_options /opt/hadoop/etc/hadoop/core-site.xml
  python3 main.py "$@"
}

main "$@"