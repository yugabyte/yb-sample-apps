FROM openjdk:8-jre-alpine
MAINTAINER YugaByte
ENV container=yb-sample-apps

WORKDIR /opt/yugabyte

ARG JAR_FILE
ADD target/${JAR_FILE} /opt/yugabyte/yb-sample-apps.jar

ENTRYPOINT ["/usr/bin/java", "-jar", "/opt/yugabyte/yb-sample-apps.jar"]
