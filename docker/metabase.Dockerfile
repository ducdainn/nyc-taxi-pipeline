FROM openjdk:11-jdk-slim-buster

ARG METABASE_VERSION=v0.49.6

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app /plugins

RUN curl -L -o /app/metabase.jar \
    "https://downloads.metabase.com/${METABASE_VERSION}/metabase.jar"

WORKDIR /app

EXPOSE 3000

ENV MB_PLUGINS_DIR=/plugins
ENV JAVA_OPTS="-Xmx2g"

CMD ["java", "-jar", "/app/metabase.jar"]


