package uk.gov.companieshouse.chsemailsender.kafka;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.EMAIL_URI;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createEmailSend;

import email.email_send;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest
public class ConsumerRetryableExceptionIT extends AbstractKafkaIT {

    @DynamicPropertySource
    public static void props(DynamicPropertyRegistry registry) {
        registry.add("steps", () -> 5);
    }

    @Test
    void testRepublishToErrorTopicThroughRetryTopics() throws Exception {
        // given
        byte[] message = writePayloadToBytes(createEmailSend(), email_send.class);

        stubFor(post(urlEqualTo(EMAIL_URI))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)));

        // when
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, 0, System.currentTimeMillis(), "key", message));
        if (!testConsumerAspect.getLatch().await(30, TimeUnit.SECONDS)) {
            fail("Timed out waiting for latch");
        }

        // then
        ConsumerRecords<?, ?> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofMillis(10000L), 6);
        assertThat(recordsPerTopic(records, MAIN_TOPIC)).isOne();
        assertThat(recordsPerTopic(records, RETRY_TOPIC)).isEqualTo(4);
        assertThat(recordsPerTopic(records, ERROR_TOPIC)).isOne();
        assertThat(recordsPerTopic(records, INVALID_TOPIC)).isZero();
    }
}
