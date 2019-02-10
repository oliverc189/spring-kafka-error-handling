package de.oc.kafkaplayground.listener;

import de.oc.kafkaplayground.exception.RecoverableException;
import de.oc.kafkaplayground.exception.UnrecoverableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class Listener {
    private static final Logger LOGGER = LoggerFactory.getLogger(Listener.class);

    private static final String RECOVERABLE = "recoverable";
    private static final String UNRECOVERABLE = "unrecoverable";


    @KafkaListener(topics = "${kafka.topic.playground}")
    public void receive(ConsumerRecord<String, String> record) {
        LOGGER.info("ConsumerRecord received:\nkey: '{}'\nvalue: '{}'", record.key(), record.value());
        if (RECOVERABLE.equals(record.value())) {
            throw new RecoverableException();
        }
        if (UNRECOVERABLE.equals(record.value())) {
            throw new UnrecoverableException();
        }
        LOGGER.info("ConsumerRecord succesfully processed");
    }

}
