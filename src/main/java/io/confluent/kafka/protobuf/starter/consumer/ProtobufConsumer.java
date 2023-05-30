package io.confluent.kafka.protobuf.starter.consumer;

import io.confluent.kafka.protobuf.starter.producer.ProtobufProducer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.protobuf.entity.CustomerMessagePayload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ProtobufConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ProtobufConsumer.class);
    private static final String TOPIC = "protobuf-topic";

    public static void main(String[] args) {
        ProtobufConsumer protobufConsumer = new ProtobufConsumer();
        protobufConsumer.readMessages();
    }

    public void readMessages() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);

        properties.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, CustomerMessagePayload.CustomerMessage.class.getName());

        KafkaConsumer<String, CustomerMessagePayload.CustomerMessage> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(TOPIC));

        //poll the record from the topic
        while (true) {
            ConsumerRecords<String, CustomerMessagePayload.CustomerMessage> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, CustomerMessagePayload.CustomerMessage> record : records) {
                logger.info("consumer first name: " + record.value().getFirstName());
                logger.info("consumer last name: " + record.value().getLastName());
            }
            consumer.commitAsync();
        }
    }
}
