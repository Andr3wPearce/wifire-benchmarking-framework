#!/usr/bin/env bash
# Build the blocks' images for linux/amd64 (NRP) and push them to Docker Hub.
#   scripts/build_wstc_image.sh                 # both: bench-core and bench-elmfire
#   scripts/build_wstc_image.sh core            # just one
#   DOCKERHUB_USER=someone scripts/build_wstc_image.sh
# Each image's tag is a hash of images/wstc/<family>/{Dockerfile,requirements.txt};
# bench.wstc.image_refs() computes the same, so a workflow file always names the matching image.
set -euo pipefail
cd "$(dirname "$0")/.."
USER_NS="${DOCKERHUB_USER:-a3pearce}"
FAMILIES=("${@:-core elmfire}")
for FAMILY in ${FAMILIES[@]}; do
  DIR="images/wstc/${FAMILY}"
  TAG=$(cat "${DIR}/Dockerfile" "${DIR}/requirements.txt" | shasum -a 1 | cut -c1-10)
  IMAGE="docker.io/${USER_NS}/bench-${FAMILY}:${TAG}"
  echo "== building ${IMAGE} (linux/amd64)"
  docker buildx build --platform linux/amd64 -t "${IMAGE}" --push "${DIR}"
  echo "== pushed ${IMAGE}"
done
echo "   make sure the Docker Hub repos ${USER_NS}/bench-* are PUBLIC (NRP pulls without credentials)"
