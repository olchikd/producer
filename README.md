Kafka Producer
==============

Test application that sends messages to Kafka.

There is a simple emulation of the books store.
It emulates 2 streams:
 * publishing the books
 * purchasing they by different people.
 
There are also console consumer.

Setup the project
-----------------

Setting JVM version:

    $ export JAVA_HOME="$(/usr/libexec/java_home -v 1.8)"

Staring Zeppeling:

    $ docker run -p 8080:8080 --rm -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_LOG_DIR='/logs' -e ZEPPELIN_NOTEBOOK_DIR='/notebook' -v /Users:/Users --name zeppelin1 f24ae811ec8d
or

    $ ./bin/zeppelin-daemon.sh start

Starting Zookeeper and Kafka:

    $ bin/zookeeper-server-start.sh config/zookeeper.properties
    $ bin/kafka-server-start.sh config/server.properties

Configuring Kafka topics:

    $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic purchases
    $ bin/kafka-topics.sh --list --zookeeper localhost:2181
    
Running console Producer:

    $ run
    
or
    
    $ runMain com.olchik.producer.ProducerApp

Also you could run console Consumer using this command:

    $ runMain com.olchik.producer.ConsumerApp