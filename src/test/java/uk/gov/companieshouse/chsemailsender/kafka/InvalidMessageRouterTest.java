package uk.gov.companieshouse.chsemailsender.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_MESSAGE;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TOPIC;

import email.email_send;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InvalidMessageRouterTest {

    private InvalidMessageRouter invalidMessageRouter;

    @Mock
    private MessageFlags flags;
    @Mock
    private email_send emailSend;

    @BeforeEach
    void setup() {
        invalidMessageRouter = new InvalidMessageRouter();
        invalidMessageRouter.configure(Map.of(
                "message-flags", flags,
                "invalid-topic", "invalid"));
    }

    private static ProducerRecord<String, Object> recordWithProvenance() {
        return new ProducerRecord<>("main", 0, "key", "an invalid message",
                List.of(new RecordHeader(ORIGINAL_TOPIC, "main".getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader(ORIGINAL_PARTITION, ByteBuffer.allocate(Integer.BYTES).putInt(3).array()),
                        new RecordHeader(ORIGINAL_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(42L).array()),
                        new RecordHeader(EXCEPTION_MESSAGE, "invalid".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void testOnSendRoutesMessageToInvalidMessageTopicIfNonRetryable() {
        // given
        ProducerRecord<String, Object> message = recordWithProvenance();

        // when
        ProducerRecord<String, Object> actual = invalidMessageRouter.onSend(message);

        // then
        assertEquals("invalid", actual.topic());
        assertNull(actual.partition());
        assertEquals("key", actual.key());
        assertEquals("an invalid message", actual.value());
        verify(flags, times(0)).destroy();
    }

    @Test
    void testOnSendPreservesProvenanceHeadersWhenRoutingToInvalidTopic() {
        // given
        ProducerRecord<String, Object> message = recordWithProvenance();

        // when
        ProducerRecord<String, Object> actual = invalidMessageRouter.onSend(message);

        // then - provenance travels with the record so it can be triaged and replayed
        assertArrayEquals("main".getBytes(StandardCharsets.UTF_8),
                actual.headers().lastHeader(ORIGINAL_TOPIC).value());
        assertEquals(3, ByteBuffer.wrap(actual.headers().lastHeader(ORIGINAL_PARTITION).value()).getInt());
        assertEquals(42L, ByteBuffer.wrap(actual.headers().lastHeader(ORIGINAL_OFFSET).value()).getLong());
        assertArrayEquals("invalid".getBytes(StandardCharsets.UTF_8),
                actual.headers().lastHeader(EXCEPTION_MESSAGE).value());
    }

    @Test
    void testOnSendToleratesMalformedProvenanceHeaders() {
        // given - headers of the wrong width must not throw, otherwise Kafka swallows the
        // exception and the record silently bypasses the invalid topic
        ProducerRecord<String, Object> message = new ProducerRecord<>("main", 0, "key", "an invalid message",
                List.of(new RecordHeader(ORIGINAL_PARTITION, new byte[]{0x03}),
                        new RecordHeader(ORIGINAL_OFFSET, new byte[0])));

        // when
        ProducerRecord<String, Object> actual = invalidMessageRouter.onSend(message);

        // then
        assertEquals("invalid", actual.topic());
    }

    @Test
    void testOnSendRoutesMessageToTargetTopicIfRetryable() {
        // given
        ProducerRecord<String, Object> message = new ProducerRecord<>("main", "key", emailSend);
        when(flags.isRetryable()).thenReturn(true);

        // when
        ProducerRecord<String, Object> actual = invalidMessageRouter.onSend(message);

        // then
        assertEquals(message, actual);
        verify(flags, times(1)).destroy();
    }

    @Test
    void testConfigureFailsFastWhenMessageFlagsIsMissing() {
        // given
        Map<String, Object> configs = Map.of("invalid-topic", "invalid");

        try (InvalidMessageRouter router = new InvalidMessageRouter()) {
            // when / then
            assertThrows(IllegalStateException.class, () -> router.configure(configs));
        }
    }

    @Test
    void testConfigureFailsFastWhenInvalidTopicIsMissing() {
        // given
        Map<String, Object> configs = Map.of("message-flags", flags);

        try (InvalidMessageRouter router = new InvalidMessageRouter()) {
            // when / then
            assertThrows(IllegalStateException.class, () -> router.configure(configs));
        }
    }
}
