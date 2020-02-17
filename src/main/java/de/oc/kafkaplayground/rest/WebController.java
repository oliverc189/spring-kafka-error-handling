package de.oc.kafkaplayground.rest;

import de.oc.kafkaplayground.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@org.springframework.web.bind.annotation.RestController
public class WebController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.playgroundTopic}")
    private String playgroundTopic;


    @RequestMapping("/")
    public String index(){
        return "Kafka Playground Rest Controller";
    }

    @PutMapping("/message/{key}")
    public String send(@PathVariable String key, @RequestBody Message message) throws InterruptedException, ExecutionException, TimeoutException {
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(playgroundTopic, key, message.getPayload());
        return send.get(2, TimeUnit.SECONDS).getRecordMetadata().toString();
    }
}
