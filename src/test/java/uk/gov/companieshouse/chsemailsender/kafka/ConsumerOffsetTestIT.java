package uk.gov.companieshouse.chsemailsender.kafka;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.EMAIL_URI;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createEmailSend;

import email.email_send;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;


@SpringBootTest
class ConsumerOffsetTestIT extends AbstractKafkaIT {

    @Autowired
    private ApplicationContext applicationContext;

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("steps", () -> 1);
        // A unique group guarantees no committed offset exists, which is the only
        // situation in which auto.offset.reset takes effect.
        registry.add("kafka.consumer.group", () -> "offset-reset-%s".formatted(UUID.randomUUID()));
    }


    @TestConfiguration
    static class DisableListenerAutoStartup {

        @Bean
        static BeanPostProcessor listenerAutoStartupDisabler() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (bean instanceof ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
                        factory.setAutoStartup(false);
                    }
                    return bean;
                }
            };
        }
    }

    @Test
    void shouldConsumeBacklogThatExistedBeforeListenerFirstStarted() throws Exception {
        // given - the Notification API accepts the email
        stubFor(post(urlEqualTo(EMAIL_URI))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.OK.value())
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)));

        // and - a record already on the topic, written before the listener ever joined the group
        byte[] message = writePayloadToBytes(createEmailSend(), email_send.class);
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, 0, System.currentTimeMillis(), "key", message));
        testProducer.flush();

        // when - the listener starts for the first time, with no committed offset
        applicationContext.getBean(KafkaListenerEndpointRegistry.class)
                .getListenerContainers()
                .forEach(MessageListenerContainer::start);

        // then - the backlog is processed rather than skipped
        if (!testConsumerAspect.getLatch().await(30, TimeUnit.SECONDS)) {
            fail("Backlog was not consumed - auto.offset.reset appears to be 'latest', not 'earliest'");
        }
        verify(1, postRequestedFor(urlEqualTo(EMAIL_URI)));
    }

}
