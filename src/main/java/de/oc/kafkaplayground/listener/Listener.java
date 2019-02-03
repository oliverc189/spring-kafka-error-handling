package de.oc.kafkaplayground.listener;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class Listener {

    @KafkaListener(topics = "${kafka.playgroundTopic}")
    public void receive(String payload){
        System.out.println("Kafka Message received. (Payload: '" + payload + "')");
    }

}
