package uk.gov.companieshouse.chsemailsender.kafka;


import email.email_send;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.EMAIL_URI;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createEmailSend;

@SpringBootTest
public class ConsumerNonRetryableExceptionIT extends AbstractKafkaIT {

    @DynamicPropertySource
    public static void props(DynamicPropertyRegistry registry) {
        registry.add("steps", () -> 1);
    }

    @Test
    void testRepublishToErrorTopicThroughNonRetryTopics() throws Exception {
        // given
        byte[] message = writePayloadToBytes(createEmailSend(), email_send.class);

        stubFor(post(urlEqualTo(EMAIL_URI))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.BAD_REQUEST.value())
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)));


        // when
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, 0, System.currentTimeMillis(), "key", message));
        if (!testConsumerAspect.getLatch().await(5, TimeUnit.SECONDS)) {
            fail("Timed out waiting for latch");
        }

        // then
        ConsumerRecords<?, ?> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofMillis(10000L), 2);
        assertThat(recordsPerTopic(records, MAIN_TOPIC)).isOne();
        assertThat(recordsPerTopic(records, RETRY_TOPIC)).isZero();
        assertThat(recordsPerTopic(records, ERROR_TOPIC)).isZero();
        assertThat(recordsPerTopic(records, INVALID_TOPIC)).isOne();
    }
}
