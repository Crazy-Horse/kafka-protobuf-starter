package io.confluent.kafka.protobuf.starter.producer;

import io.confluent.kafka.protobuf.starter.model.MessageRequest;
import io.confluent.protobuf.entity.CustomerMessagePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.UUID;

@Service
public class ProtobufSpringProducer {

    private static final Logger logger = LoggerFactory.getLogger(ProtobufSpringProducer.class);
    private static final String TOPIC = "protobuf-topic";

    @Autowired
    private KafkaTemplate<String, CustomerMessagePayload.CustomerMessage> kafkaTemplate;

    public void sendMessage(MessageRequest message) {
        logger.info(String.format("#### -> Producing message -> %s", message));

        CustomerMessagePayload.CustomerMessage messageCustomer  = CustomerMessagePayload.CustomerMessage
                .newBuilder()
                .setFirstName(message.getFirstName())
                .setLastName(message.getLastName())
                .setId(UUID.randomUUID().toString())
                .build();

        this.kafkaTemplate.send(TOPIC, messageCustomer.getId(), messageCustomer);
    }
}
