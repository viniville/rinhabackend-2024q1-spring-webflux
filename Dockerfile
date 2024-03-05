FROM eclipse-temurin:21-jre-jammy
LABEL authors="vinicius"
ARG JAR_FILE
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]