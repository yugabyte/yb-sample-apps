FROM eclipse-temurin:8u462-b08-jre-alpine-3.21
MAINTAINER YugaByte
ENV container=yb-sample-apps

WORKDIR /opt/yugabyte

ARG JAR_FILE
ADD target/${JAR_FILE} /opt/yugabyte/yb-sample-apps.jar

ENTRYPOINT ["/usr/bin/java", "-jar", "/opt/yugabyte/yb-sample-apps.jar"]
