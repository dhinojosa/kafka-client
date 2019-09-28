export CONFLUENT_HOME=~/confluent-5.0.0
$CONFLUENT_HOME/bin/kafka-topics \
--create --zookeeper localhost:2181 \
--replication-factor 1 --partitions 3 \
--topic mysql-kafka_order
