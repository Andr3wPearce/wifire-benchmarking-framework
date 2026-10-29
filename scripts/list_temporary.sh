#!/usr/bin/env bash
# Everything tagged TEMP(<tag>) that must go before this is the finished framework.
# See docs/TEMPORARY.md. Exits non-zero while anything is left, so CI can gate on it.
cd "$(dirname "$0")/.."
hits=$(grep -rnI --exclude-dir=.git --exclude-dir=build --exclude-dir=__pycache__ \
        --exclude=TEMPORARY.md --exclude=list_temporary.sh -e 'TEMP(' . | sort)
if [ -z "$hits" ]; then echo "nothing temporary left"; exit 0; fi
grep -o 'TEMP([a-z-]*)' <<< "$hits" | sort | uniq -c
echo "$hits" | sed 's/^\.\//  /' | cut -c1-150
exit 1
