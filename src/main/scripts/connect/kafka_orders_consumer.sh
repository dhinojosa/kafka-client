export CONFLUENT_HOME=~/confluent-5.0.0
$CONFLUENT_HOME/bin/kafka-avro-console-consumer \
--bootstrap-server localhost:9092 \
--property schema.registry.url=http://localhost:8081 \
--topic mysql-kafka_order \
--from-beginning
