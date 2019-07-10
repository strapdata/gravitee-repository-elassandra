#!/bin/bash
#
# Copyright (C) 2019 Strapdata (https://www.strapdata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Build gravitee docker images including the elassandra repository.
#
set -ex

GRAVITEE_VERSION=${GRAVITEE_VERSION:-"1.27.1"}

DOCKER_BUILD_OPTS=${DOCKER_BUILD_OPTS:-"--no-cache --rm"}

# set the target docker registry and repository
DOCKER_REPO="strapdata"

# If set, the images will be published to docker hub
DOCKER_PUBLISH=${DOCKER_PUBLISH:-false}

# If set, the images will be tagged latest  
DOCKER_LATEST=${DOCKER_LATEST:-true}

# If set, the image is considered to be the latest relative to the major version.
# Consequently, the image will be tagged with generic versions (for instance 6.2.3.4 will produce 6, 6.2 and 6.2.3)
DOCKER_MAJOR_LATEST=${DOCKER_MAJOR_LATEST:-true}

push() {
  if [ "$DOCKER_PUBLISH" = true ]; then
    echo "Publishing $1"
    docker push ${1}
  fi
}

# $1 = docker image
# $2 = docker tag
tag_and_push() {
  local image=$1
  local tag=$2
  docker tag ${image}:${GRAVITEE_VERSION} ${image}:${tag}
  push ${image}:${tag}
}

# $1 = docker image
publish() {
 local image=$1
 # tag and publish image if DOCKER_PUBLISH=true
  push "${image}:${GRAVITEE_VERSION}"

  if [ "$DOCKER_LATEST" = "true" ]; then
    tag_and_push "${image}" latest
  fi

  if [ "$DOCKER_MAJOR_LATEST" = "true" ]; then
#    tag_and_push "${image}" "${GRAVITEE_VERSION%.*.*.*}" # three digit version
    tag_and_push "${image}" "${GRAVITEE_VERSION%.*.*}" # two digit version
    tag_and_push "${image}" "${GRAVITEE_VERSION%.*}" # one digit version
  fi
}

for component in gateway management-api
do
    image="${DOCKER_REPO}/graviteeio-${component}"
    docker build ${DOCKER_BUILD_OPTS} --build-arg GRAVITEE_TAG=${GRAVITEE_VERSION} -f Dockerfile.$component -t "${image}:$GRAVITEE_VERSION" .
    publish "${image}"
done
