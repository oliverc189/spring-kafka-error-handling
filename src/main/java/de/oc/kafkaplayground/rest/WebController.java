package de.oc.kafkaplayground.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@org.springframework.web.bind.annotation.RestController
public class WebController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.playground}")
    private String playgroundTopic;


    @RequestMapping("/")
    public String index(){
        return "Kafka Playground Rest Controller";
    }

    @GetMapping("/send/{payload}")
    public String send(@PathVariable String payload) throws InterruptedException, ExecutionException, TimeoutException {
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(playgroundTopic, payload);
        return send.get(2, TimeUnit.SECONDS).getRecordMetadata().toString();
    }
}
