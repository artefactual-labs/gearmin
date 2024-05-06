#!/usr/bin/env fish

# This script repeatedly runs the specified Go test command until it fails.

while true
    go test -run=TestWithHighLoad -count=1 -v -timeout=10s
    if test $status -ne 0
        echo "Command failed"
        break
    end
end
