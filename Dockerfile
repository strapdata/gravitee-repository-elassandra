ARG TAG
ARG IMAGE

FROM $IMAGE:$TAG
MAINTAINER Strapdata Team <http://www.strapdata.com>

COPY target/gravitee-repository-elassandra-*.zip $GRAVITEEIO_HOME/plugins/