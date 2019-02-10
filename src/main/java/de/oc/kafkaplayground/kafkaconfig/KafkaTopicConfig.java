package de.oc.kafkaplayground.kafkaconfig;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value("${kafka.topic.playground}")
    private String playgroundTopic;

    @Value("${kafka.topic.deadletter.suffix.recoverable}")
    private String recoverableDltSuffix;

    @Value("${kafka.topic.deadletter.suffix.unrecoverable}")
    private String unrecoverableDltSuffix;

    @Bean
    public KafkaAdmin kafkaAdmin(){
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(config);
    }

    @Bean
    public NewTopic newPlaygroundTopic(){
        return new NewTopic(playgroundTopic, 1 , (short) 1);
    }

    @Bean
    public NewTopic newRecoverableDltTopic(){
        return new NewTopic(playgroundTopic + recoverableDltSuffix, 1 , (short) 1);
    }

    @Bean
    public NewTopic newUnrecoverableDltTopic(){
        return new NewTopic(playgroundTopic + unrecoverableDltSuffix, 1 , (short) 1);
    }

}
