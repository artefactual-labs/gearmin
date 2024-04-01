#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__root="$(cd "$(dirname "${__dir}")" && pwd)"

covermode=${COVERMODE:-atomic}
coverprofile=$(mktemp /tmp/coverage.XXXXXXXXXX)

cd ${__root}
go test -coverpkg=./... -coverprofile="${coverprofile}" -covermode="${covermode}" ./...
go tool cover -func "${coverprofile}"

case "${1-}" in
    --html)
        go tool cover -html "${coverprofile}"
        ;;
esac
