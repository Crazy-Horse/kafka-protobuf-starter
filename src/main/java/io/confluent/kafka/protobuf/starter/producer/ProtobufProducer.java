package io.confluent.kafka.protobuf.starter.producer;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.protobuf.entity.CustomerMessagePayload;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
This is a standard Kafka producer without using Spring Boot features
 */
public class ProtobufProducer {

    private static final Logger logger = LoggerFactory.getLogger(ProtobufProducer.class);
    private static final String TOPIC = "protobuf-topic";

    public static void main(String[] args) {
        ProtobufProducer protobufProducer = new ProtobufProducer();
        protobufProducer.writeMessage();
    }

    public void writeMessage() {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        properties.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        Producer<String, CustomerMessagePayload.CustomerMessage> producer = new KafkaProducer<>(properties);

        //prepare the message
        CustomerMessagePayload.CustomerMessage simpleMessage =
                CustomerMessagePayload.CustomerMessage.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setFirstName("John")
                        .setLastName("Doe")
                        .build();

        logger.info(String.valueOf(simpleMessage));

        //prepare the kafka record
        ProducerRecord<String, CustomerMessagePayload.CustomerMessage> record
                = new ProducerRecord<>(TOPIC, "1234", simpleMessage);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }
}
