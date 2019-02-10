package de.oc.kafkaplayground.kafkaconfig;

import de.oc.kafkaplayground.exception.RecoverableException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

@EnableKafka
@Configuration
public class KafkaListenerConfig {
    @Value("${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value("${kafka.groupId}")
    private String groupId;

    @Value("${kafka.topic.deadletter.suffix.recoverable}")
    private String recoverableDltSuffix;

    @Value("${kafka.topic.deadletter.suffix.unrecoverable}")
    private String unrecoverableDltSuffix;

    @Autowired
    private KafkaTemplate<Object, Object> dltKafkaTemplate;

    @Bean
    public ConsumerFactory<String, String> consumerFactory(){
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(){
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setErrorHandler(seekToErrorHandler());
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    private ErrorHandler seekToErrorHandler() {
        return new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer(), 3);
    }

    private BiConsumer<ConsumerRecord<?,?>, Exception> deadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(dltKafkaTemplate, (consumerRecord, e) -> {
           if (e.getCause() instanceof RecoverableException){
                return new TopicPartition(consumerRecord.topic() + recoverableDltSuffix, consumerRecord.partition());
           }
           return new TopicPartition(consumerRecord.topic() + unrecoverableDltSuffix, consumerRecord.partition());
        });
    }

}
