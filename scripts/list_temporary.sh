#!/usr/bin/env bash
# Everything that must go (or change) before deployment. See docs/TEMPORARY.md.
# Exits non-zero while anything is left, so CI can gate a release on it.
cd "$(dirname "$0")/.."
hits=$(grep -rnI --exclude-dir=.git --exclude-dir=artifacts --exclude-dir=.venv --exclude-dir=.dagster_home --exclude-dir=submissions --exclude-dir=__pycache__ --exclude=TEMPORARY.md \
        --exclude=list_temporary.sh -e 'TEMP(' -e 'registry\.example' . | sort)
if [ -z "$hits" ]; then echo "nothing temporary left"; exit 0; fi
for tag in dry-run local-dev api-stand-in placeholder; do
  pat="TEMP($tag)"; [ $tag = placeholder ] && pat='registry.example'
  n=$(grep -cF "$pat" <<< "$hits")
  [ "$n" -gt 0 ] && { echo "== $tag ($n)"; grep -F "$pat" <<< "$hits" | sed 's/^\.\//  /' | cut -c1-140; }
done
exit 1
