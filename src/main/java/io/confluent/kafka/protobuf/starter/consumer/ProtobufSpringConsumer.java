package io.confluent.kafka.protobuf.starter.consumer;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.protobuf.entity.CustomerMessagePayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ProtobufSpringConsumer {

    private final Logger logger = LoggerFactory.getLogger(ProtobufSpringConsumer.class);
    private static final String TOPIC = "protobuf-topic";

    @KafkaListener(topics = TOPIC, groupId = "group_id_v1")
    public void processEvent(ConsumerRecord<String, DynamicMessage> record) {

        CustomerMessagePayload.CustomerMessage message = null;
        try {
            message = CustomerMessagePayload.CustomerMessage.newBuilder().build().getParserForType().parseFrom(record.value().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return;
        }

        logger.info(String.format("#### -> Consuming message -> %s", message.toString()));
    }
}
