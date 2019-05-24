FROM openjdk:8
COPY ./out/artifacts/loader_jar/* /tmp/
WORKDIR /tmp
ENTRYPOINT ["java","-jar","loader.jar"]