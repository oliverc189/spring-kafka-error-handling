package de.oc.kafkaplayground.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
public class RecoverableDltListener {

    public static final String ID_RECOVERABLE_DLT_LISTENER = "recoverable.dlt.listener";

    @KafkaListener(id = ID_RECOVERABLE_DLT_LISTENER, topics = "${kafka.topic.playground}" + "${kafka.topic.deadletter.suffix.recoverable}")
    public void processDlt(ConsumerRecord<String, String> record) {
        System.out.println("Dead Letter Record recovered, value: '" + record.value() + "'.");
    }

    @Bean
    public ApplicationRunner scheduler(KafkaListenerEndpointRegistry listenerRegistry) {
        return args -> {
            while (true) {
                listenerRegistry.getListenerContainer(ID_RECOVERABLE_DLT_LISTENER).pause();
                Thread.sleep(60000); // 1 min
                listenerRegistry.getListenerContainer(ID_RECOVERABLE_DLT_LISTENER).resume();
                Thread.sleep(60000); // 1 min
            }
        };
    }
}
