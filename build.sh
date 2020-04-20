#!/bin/bash
#
# This file is part of Gravitee.io APIM - API Management - Repository for Elassandra.
#
# Gravitee.io APIM - API Management - Repository for Elassandra is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Gravitee.io APIM - API Management - Repository for Elassandra is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Gravitee.io APIM - API Management - Repository for Elassandra.  If not, see <http://www.gnu.org/licenses/>.
#

#
# Build gravitee docker images including the elassandra repository.
#
set -ex

GRAVITEE_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

DOCKER_BUILD_OPTS=${DOCKER_BUILD_OPTS:-"--no-cache --rm"}

# set the target docker registry and repository
DOCKER_REPO=${DOCKER_REPO:-"strapdata"}

# If set, the images will be published to docker hub
DOCKER_PUBLISH=${DOCKER_PUBLISH:-false}

# If set, the images will be tagged latest
DOCKER_LATEST=${DOCKER_LATEST:-true}

# If set, the image is considered to be the latest relative to the major version.
# Consequently, the image will be tagged with generic versions (for instance 6.2.3.4 will produce 6, 6.2 and 6.2.3)
DOCKER_MAJOR_LATEST=${DOCKER_MAJOR_LATEST:-true}

# Dockerfile
DOCKERFILE=${DOCKERFILE:-"Dockerfile"}

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


curl -o target/lucene-analyzers-common-7.7.2.jar "https://repo1.maven.org/maven2/org/apache/lucene/lucene-analyzers-common/7.7.2/lucene-analyzers-common-7.7.2.jar" 2>/dev/null
curl -o target/lucene-core-7.7.2.jar "https://repo1.maven.org/maven2/org/apache/lucene/lucene-core/7.7.2/lucene-core-7.7.2.jar" 2>/dev/null
curl -o target/lucene-queries-7.7.2.jar "https://repo1.maven.org/maven2/org/apache/lucene/lucene-queries/7.7.2/lucene-queries-7.7.2.jar" 2>/dev/null
curl -o target/lucene-queryparser-7.7.2.jar "https://repo1.maven.org/maven2/org/apache/lucene/lucene-queryparser/7.7.2/lucene-queryparser-7.7.2.jar" 2>/dev/null
curl -o target/lucene-sandbox-7.7.2.jar "https://repo1.maven.org/maven2/org/apache/lucene/lucene-sandbox/7.7.2/lucene-sandbox-7.7.2.jar" 2>/dev/null

for component in gateway management-api
do
    image="${DOCKER_REPO}/graviteeio-${component}"
    docker build  . ${DOCKER_BUILD_OPTS} --build-arg TAG=${GRAVITEE_VERSION} \
                                      --build-arg IMAGE=graviteeio/${component} \
                                      -f $DOCKERFILE"."${component} -t "${image}:$GRAVITEE_VERSION"
    publish "${image}"
done
