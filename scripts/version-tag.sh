#!/bin/bash
buildtime=$(date -u +%Y-%m-%dT%H:%M:%S%z)
githash=$(git rev-parse HEAD)
shorthash=$(git rev-parse --short HEAD)
builddate=$(date -u +%Y%m%d)

go_version (){
    v=$( go version )
    if [ $? -ne 0 ]  ; then
        log "could not get go version"
        exit -1
    fi
    echo $v
}

goversion=$( go_version )
