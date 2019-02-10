package de.oc.kafkaplayground.listener;

import de.oc.kafkaplayground.exception.RecoverableException;
import de.oc.kafkaplayground.exception.UnrecoverableException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class Listener {
    private static final String RECOVERABLE = "recoverable";
    private static final String UNRECOVERABLE = "unrecoverable";


    @KafkaListener(topics = "${kafka.topic.playground}")
    public void receive(String payload){
        System.out.println("Kafka Message received. (Payload: '" + payload + "')");
        if(RECOVERABLE.equals(payload)){
            throw new RecoverableException();
        }
        if(UNRECOVERABLE.equals(payload)){
            throw new UnrecoverableException();
        }
    }

}
