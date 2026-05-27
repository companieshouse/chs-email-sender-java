package uk.gov.companieshouse.chsemailsender.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class ConsumerInvalidPayloadExceptionIT extends AbstractKafkaIT {

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("steps", () -> 1);
    }

    @Test
    void testPublishToEmailSendInvalidMessageTopicIfInvalidDataDeserialised() {
        // given
        byte[] message = writePayloadToBytes("bad data", String.class);

        // when
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, 0, System.currentTimeMillis(), "key", message));

        // then
        ConsumerRecords<?, ?> consumerRecords = KafkaTestUtils.getRecords(testConsumer, Duration.ofMillis(10000L), 2);
        assertThat(recordsPerTopic(consumerRecords, MAIN_TOPIC)).isOne();
        assertThat(recordsPerTopic(consumerRecords, RETRY_TOPIC)).isZero();
        assertThat(recordsPerTopic(consumerRecords, ERROR_TOPIC)).isZero();
        assertThat(recordsPerTopic(consumerRecords, INVALID_TOPIC)).isOne();
        verify(0, anyRequestedFor(anyUrl()));
    }
}
