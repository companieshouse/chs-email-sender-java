package uk.gov.companieshouse.chsemailsender.kafka;

import email.email_send;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createEmailSend;

@SpringBootTest
class ConsumerPositiveIT extends AbstractKafkaIT {

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("steps", () -> 1);
    }

    @Test
    void shouldConsumeEmailSendMessagesAndProcessSuccessfully() throws Exception {
        byte[] message = writePayloadToBytes(createEmailSend(), email_send.class);

        // when
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, 0, System.currentTimeMillis(), "key", message));
        if (!testConsumerAspect.getLatch().await(10, TimeUnit.SECONDS)) {
            fail("Timed out waiting for latch");
        }
        ConsumerRecords<?, ?> consumerRecords = KafkaTestUtils.getRecords(testConsumer, Duration.ofMillis(10000L), 1);
        assertThat(recordsPerTopic(consumerRecords, MAIN_TOPIC)).isOne();
        assertThat(recordsPerTopic(consumerRecords, RETRY_TOPIC)).isZero();
        assertThat(recordsPerTopic(consumerRecords, ERROR_TOPIC)).isZero();
        assertThat(recordsPerTopic(consumerRecords, INVALID_TOPIC)).isZero();
    }
}


