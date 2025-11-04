FROM eclipse-temurin:8u462-b08-jre-alpine
MAINTAINER YugaByte
ENV container=yb-sample-apps

# Required to get ssl connections working:
# https://github.com/adoptium/containers/issues/319
RUN apk add --no-cache libgcc

WORKDIR /opt/yugabyte

ARG JAR_FILE
ADD target/${JAR_FILE} /opt/yugabyte/yb-sample-apps.jar

ENTRYPOINT ["/usr/bin/java", "-jar", "/opt/yugabyte/yb-sample-apps.jar"]
