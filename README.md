# kafka saver

Simple service template.

Consumes messages from Kafka and stores them in DataBase
## Run Kafka + Zookepper

This configuration fits most development requirements.

    - Zookeeper will be available at $DOCKER_HOST_IP:2181
    - Kafka will be available at $DOCKER_HOST_IP:9092
    - (experimental) JMX port at $DOCKER_HOST_IP:9999


Run with:
```
docker compose up
docker compose down
```

