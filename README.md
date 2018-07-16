Kafka Producer
==============

Sends random records to Kafka.

Setup the project
-----------------

Setting JVM version:

    $ export JAVA_HOME="$(/usr/libexec/java_home -v 1.8)"

Staring Zeppeling under Docker:

    $ docker run -p 8080:8080 --rm -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_LOG_DIR='/logs' -e ZEPPELIN_NOTEBOOK_DIR='/notebook' -v /Users:/Users --name zeppelin1 f24ae811ec8d

Starting Zookeeper and Kafka:

    $ bin/zookeeper-server-start.sh config/zookeeper.properties
    $ bin/kafka-server-start.sh config/server.properties

Configuring Kafka topics:

    $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    $ bin/kafka-topics.sh --list --zookeeper localhost:2181
    
Running console Producer:

    $ run
    
or
    
    $ runMain com.olchik.producer.ProducerApp

Also you could run console Consumer using this command:

    $ runMain com.olchik.producer.ConsumerApp