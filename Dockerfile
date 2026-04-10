FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom.xml .
COPY src ./src

RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/app.jar /app/app.jar

ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

ENTRYPOINT ["java", "-cp", "/app/app.jar"]
CMD ["io.redis.devrel.demo.eda.producer.TransactionProducer"]
