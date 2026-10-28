#!/usr/bin/env bash
# TEMP(local-dev): builds into minikube; deployment pushes images to a registry (CI).
# Build every real block's image straight into minikube (no registry needed).
# Usage: scripts/build_blocks.sh [block ...]     default: all blocks under images/
set -e
export PATH="$HOME/.homebrew/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
REPO="$(cd "$(dirname "$0")/.." && pwd)"; cd "$REPO"
command -v minikube >/dev/null || { echo "minikube not found"; exit 1; }
minikube status >/dev/null 2>&1 || { echo "minikube isn't running: start it with 'minikube start'"; exit 1; }
blocks=("$@"); [ ${#blocks[@]} -eq 0 ] && blocks=($(ls images))
for b in "${blocks[@]}"; do
  image=$(python3 -c "import yaml,sys;print(yaml.safe_load(open('blocks/$b.yaml'))['execution']['image'])" 2>/dev/null \
          || grep -A2 '^execution:' "blocks/$b.yaml" | awk '/image:/{print $2; exit}')
  echo "== building $image from images/$b (first build compiles pyretechnics: a few minutes)"
  minikube image build -t "$image" "images/$b"
  # `minikube image build` can exit 0 even when the build fails, so check for the image
  if ! minikube image ls 2>/dev/null | grep -q "$image"; then
    echo "!! build of $image FAILED (see the output above)"; exit 1
  fi
  echo "== $image is in minikube"
done
minikube image ls | grep -E "bench/" || true
