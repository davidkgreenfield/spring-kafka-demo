package com.dbg.springkafkademo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.HashMap;
import java.util.Map;


@Slf4j
@EnableKafka
@Configuration
@SpringBootApplication
public class SpringKafkaDemoApplication {

    /*
    This will be the number of ListenerContainers, each with it's own consumer and listener.
    When you are using a group coordinator each will take up a set of partitions, up to the number of partitions
    in the topic subscribed to.
     */
    private static final int CONCURENCY = 6;


    /*
    To use this ContainerFactory you must specify it with the containerFactory of the @KafkaListener annotation
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory myKafkaListenerContainerFactory(@Autowired ConsumerFactory consumerFactory) {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);

        /*
        NOTE:  Any consumer using this factory will have the this concurrency.
        If you want to have a different concurrency level for a listener, create a new factory and give it the
        desired concurrency setting.
        will specify the specific factory in it's annotation.
         */
        factory.setConcurrency(CONCURENCY);
        return factory;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return propsMap;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaDemoApplication.class, args);
    }

    /*
    The following 2 listeners are part of the same consumer group.  They cannot listen to the same partition of the same topic.
    test_listener_id_1 is listening to test-topic-2 partition 1 and test_listener_id_2 is listening to partition 0 of that topic.
    If they were specified to listen to the same topic and partition, only one would get assigned that topic and partition and the other
    would sit idle.

    kafka-topics --create --topic test-topic-1 --zookeeper localhost:2181 --replication-factor 1
    kafka-topics --create --topic test-topic-2 --partitions 2 --zookeeper localhost:2181 --replication-factor 1
     */
    /*
    This listener will listen to partition 0 on test-topic-1 and partition 1 on test-topic-2
     */
    @KafkaListener(id = "test_listener_id_1",
            groupId = "test_group_1",
            containerFactory = "myKafkaListenerContainerFactory",
            topicPartitions = {@TopicPartition(topic = "test-topic-1", partitions = {"0"}), @TopicPartition(topic = "test-topic-2", partitions = {"1"})})
    public void processMessage(String message,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionId) {
        log.info("message Listener 1 is " + message);
    }

    /*
    This listener will listen to partition 0 of test-topic-2
     */
    @KafkaListener(id = "test_listener_id_2",
            groupId = "test_group_1",
            containerFactory = "myKafkaListenerContainerFactory",
            topicPartitions = @TopicPartition(topic = "test-topic-2", partitions = {"0"}))
    public void processMessage1(String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionId) {
        log.info("message Listener 2 is " + message);
    }

    /*
    This listener will use the custom KafkaListenerContainerFactory
    kafka-topics --create --topic test-topic-6 --partitions 6 --zookeeper localhost:2181 --replication-factor 1
     */
    @KafkaListener(id = "test_listener_id_3",
            groupId = "test_group_1",
            containerFactory = "myKafkaListenerContainerFactory",
            topics = {"test-topic-6"})
    public void processMessage3(@Payload String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionId) {
        log.info("thread " + Thread.currentThread().getName());
        log.info("message consumer partition " + partitionId + " is " + message);
    }
}
