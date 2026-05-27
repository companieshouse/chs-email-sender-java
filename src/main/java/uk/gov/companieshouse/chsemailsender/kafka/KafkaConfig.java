package uk.gov.companieshouse.chsemailsender.kafka;

import email.email_send;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;
import uk.gov.companieshouse.chsemailsender.serdes.KafkaPayloadDeserialiser;
import uk.gov.companieshouse.chsemailsender.serdes.KafkaPayloadSerialiser;

import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String kafkaBrokers;

    @Bean
    public ConsumerFactory<String, email_send> messageReceiveConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
                        ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class,
                        ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaPayloadDeserialiser.class,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
                ),
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new KafkaPayloadDeserialiser<>(email_send.class)));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, email_send> listenerContainerFactory(
            ConsumerFactory<String, email_send> consumerFactory,
            @Value("${kafka.consumer.concurrency}") Integer concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, email_send> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }


    @Bean
    public ProducerFactory<String, Object> producerFactory(MessageFlags messageFlags,
                                                           @Value("${kafka.consumer.topic}") String topic,
                                                           @Value("${kafka.consumer.group}") String groupId) {
        return new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers,
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DelegatingByTypeSerializer.class,
                        ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, InvalidMessageRouter.class.getName(),
                        "message-flags", messageFlags,
                        "invalid-topic", "%s-%s-invalid".formatted(topic, groupId)),
                new StringSerializer(), new DelegatingByTypeSerializer(
                Map.of(
                        byte[].class, new ByteArraySerializer(),
                        email_send.class, new KafkaPayloadSerialiser<>(email_send.class))));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public RetryTopicConfiguration retryTopicConfiguration(KafkaTemplate<String, Object> template,
                                                           @Value("${kafka.consumer.group}") String groupId,
                                                           @Value("${kafka.consumer.retry.max-attempts}") int attempts,
                                                           @Value("${kafka.consumer.retry.backoff-delay-ms}") int delay) {
        return RetryTopicConfigurationBuilder
                .newInstance()
                .doNotAutoCreateRetryTopics() // this is necessary to prevent failing connection during loading of spring app context
                .maxAttempts(attempts)
                .fixedBackOff(delay)
                .useSingleTopicForSameIntervals()
                .retryTopicSuffix("-%s-retry".formatted(groupId))
                .dltSuffix("-%s-error".formatted(groupId))
                .dltProcessingFailureStrategy(DltStrategy.FAIL_ON_ERROR)
                .retryOn(RetryableException.class)
                .create(template);
    }

}