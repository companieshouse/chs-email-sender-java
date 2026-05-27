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
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.EMAIL_URI;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createEmailSend;

@SpringBootTest
class ConsumerPositiveIT extends AbstractKafkaIT {


    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("steps", () -> 1);
    }

    @Test
    void shouldConsumeEmailSendMessageAndPostToNotificationApiSuccessfully() throws Exception {
        // given
        stubFor(post(urlEqualTo(EMAIL_URI))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.OK.value())
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)));

        byte[] message = writePayloadToBytes(createEmailSend(), email_send.class);

        // when
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, 0, System.currentTimeMillis(), "key", message));
        if (!testConsumerAspect.getLatch().await(10, TimeUnit.SECONDS)) {
            fail("Timed out waiting for latch");
        }

        // then
        ConsumerRecords<?, ?> consumerRecords = KafkaTestUtils.getRecords(testConsumer, Duration.ofMillis(10000L), 1);
        assertThat(recordsPerTopic(consumerRecords, MAIN_TOPIC)).isOne();
        assertThat(recordsPerTopic(consumerRecords, RETRY_TOPIC)).isZero();
        assertThat(recordsPerTopic(consumerRecords, ERROR_TOPIC)).isZero();
        assertThat(recordsPerTopic(consumerRecords, INVALID_TOPIC)).isZero();
        verify(1, postRequestedFor(urlEqualTo(EMAIL_URI))
                .withHeader("Content-Type", equalTo(MediaType.APPLICATION_JSON_VALUE))
                .withHeader("appID", equalTo("penalty-payment-api.sanctions_roe_penalty_payment_received_email"))
                .withHeader("templateName", equalTo("sanctions_roe_penalty_payment_received_email"))
                .withRequestBody(containing("payable_resource"))
                .withRequestBody(containing("customer_code")));
    }
}


