package uk.gov.companieshouse.chsemailsender.kafka;

import email.message_send;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_MESSAGE;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_PARTITION;

@ExtendWith(MockitoExtension.class)
class InvalidMessageRouterTest {

    private InvalidMessageRouter invalidMessageRouter;

    @Mock
    private MessageFlags flags;
    @Mock
    private message_send messageSend;

    @BeforeEach
    void setup() {
        invalidMessageRouter = new InvalidMessageRouter();
        invalidMessageRouter.configure(Map.of(
                "message-flags", flags,
                "invalid-topic", "invalid"));
    }

    @Test
    void testOnSendRoutesMessageToInvalidMessageTopicIfNonRetryable() {
        // given
        ProducerRecord<String, Object> message = new ProducerRecord<>("main", 0, "key", "an invalid message",
                List.of(new RecordHeader(ORIGINAL_PARTITION, BigInteger.ZERO.toByteArray()),
                        new RecordHeader(ORIGINAL_OFFSET, BigInteger.ONE.toByteArray()),
                        new RecordHeader(EXCEPTION_MESSAGE, "invalid".getBytes())));
        // when
        ProducerRecord<String, Object> actual = invalidMessageRouter.onSend(message);

        // then
        assertEquals(new ProducerRecord<>("invalid", "key", "an invalid message"), actual);
        verify(flags, times(0)).destroy();
    }

    @Test
    void testOnSendRoutesMessageToTargetTopicIfRetryable() {
        // given
        ProducerRecord<String, Object> message = new ProducerRecord<>("main", "key", messageSend);
        when(flags.isRetryable()).thenReturn(true);

        // when
        ProducerRecord<String, Object> actual = invalidMessageRouter.onSend(message);

        // then
        assertEquals(message, actual);
        verify(flags, times(1)).destroy();
    }
}
