package de.oc.kafkaplayground.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
public class RecoverableDltListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverableDltListener.class);

    public static final String ID_RECOVERABLE_DLT_LISTENER = "recoverable.dlt.listener";

    @KafkaListener(id = ID_RECOVERABLE_DLT_LISTENER, topics = "${kafka.topic.playground}" + "${kafka.topic.deadletter.suffix.recoverable}")
    public void processDlt(ConsumerRecord<String, String> record) {
        LOGGER.info("ConsumerRecord recovered:\nkey: '{}'\nvalue: '{}'\nDLT Exception Message: {}",
                record.key(),
                record.value(),
                new String(record.headers().lastHeader("kafka_dlt-exception-message").value()));
    }

    @Bean
    public ApplicationRunner scheduler(KafkaListenerEndpointRegistry listenerRegistry) {
        return args -> {
            while (true) {
                listenerRegistry.getListenerContainer(ID_RECOVERABLE_DLT_LISTENER).pause();
                LOGGER.info(ID_RECOVERABLE_DLT_LISTENER + " paused");
                Thread.sleep(20000);
                listenerRegistry.getListenerContainer(ID_RECOVERABLE_DLT_LISTENER).resume();
                LOGGER.info(ID_RECOVERABLE_DLT_LISTENER + " resumed");
                Thread.sleep(20000);
            }
        };
    }
}
