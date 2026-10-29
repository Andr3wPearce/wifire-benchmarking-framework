#!/usr/bin/env bash
# Build the blocks' image for linux/amd64 (NRP) and push it to Docker Hub.
#   scripts/build_wstc_image.sh            # pushes docker.io/a3pearce/bench-blocks:<tag>
#   DOCKERHUB_USER=someone scripts/build_wstc_image.sh
# The tag is a hash of images/blocks/{Dockerfile,requirements.txt}; bench.wstc.image_ref()
# computes the same one, so the workflow file always names the image that matches.
set -euo pipefail
cd "$(dirname "$0")/.."
USER_NS="${DOCKERHUB_USER:-a3pearce}"
TAG=$(cat images/blocks/Dockerfile images/blocks/requirements.txt | shasum -a 1 | cut -c1-10)
IMAGE="docker.io/${USER_NS}/bench-blocks:${TAG}"
echo "== building ${IMAGE} (linux/amd64; first build compiles pyretechnics, ~10 min on a Mac)"
docker buildx build --platform linux/amd64 -t "${IMAGE}" --push images/blocks
echo "== pushed ${IMAGE}"
echo "   make sure the Docker Hub repo ${USER_NS}/bench-blocks is PUBLIC (NRP pulls without credentials)"
