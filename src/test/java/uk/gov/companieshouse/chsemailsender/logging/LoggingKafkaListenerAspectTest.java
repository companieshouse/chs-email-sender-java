package uk.gov.companieshouse.chsemailsender.logging;

import email.message_send;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS;
import static org.springframework.kafka.support.KafkaHeaders.OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC;

@ExtendWith(MockitoExtension.class)
class LoggingKafkaListenerAspectTest {

    private static final String TOPIC = "message-send";

    private static final Pattern INFO_EVENT_PATTERN = Pattern.compile(
            "event: info|\"event\":\"info\"");
    private static final Pattern ERROR_EVENT_PATTERN = Pattern.compile(
            "event: error|\"event\":\"error\"");
    private static final Pattern MAX_RETRY_ATTEMPTS_REACHED_PATTERN = Pattern.compile(
            "error: Max retry attempts reached|\"message\":\"Max retry attempts reached\"");
    private static final Pattern INVALID_PAYLOAD_PATTERN = Pattern.compile(
            "error: Invalid payload type, payload: \\[message payload]|\"message\":\"Invalid payload type, payload: \\[message payload]\"");
    private static final Pattern REQUEST_ID_INITIALISED_PATTERN = Pattern.compile(
            "request_id: message-send-0-0|\"request_id\":\"message-send-0-0\"");
    private static final Pattern RETRY_COUNT_ZERO_PATTERN = Pattern.compile(
            "retry_count: 0|\"retry_count\":0");
    private static final Pattern RETRY_COUNT_FOUR_PATTERN = Pattern.compile(
            "retry_count: 4|\"retry_count\":4");
    private static final Pattern MAIN_TOPIC_PATTERN = Pattern.compile(
            "topic: message-send|\"topic\":\"message-send\"");
    private static final Pattern PARTITION_ZERO_PATTERN = Pattern.compile(
            "partition: 0|\"partition\":0");
    private static final Pattern OFFSET_ZERO_PATTERN = Pattern.compile(
            "offset: 0|\"offset\":0");
    private static final String MESSAGE_ID = "123456789";
    private static final Pattern CONTEXT_ID_PATTERN = Pattern.compile(
            "context_id: %s|\"context_id\":\"%s\"".formatted(MESSAGE_ID, MESSAGE_ID));

    private LoggingKafkaListenerAspect aspect;

    @Mock
    private ProceedingJoinPoint joinPoint;

    @Mock
    private Message<message_send> messageSendMessage;
    @Mock
    private message_send messageSend;
    @Mock
    private Message<String> invalidMessage;

    @BeforeEach
    void setUp() {
        aspect = new LoggingKafkaListenerAspect(5);
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void shouldManageMessageSendStructuredLogging(CapturedOutput capture) throws Throwable {
        // given
        MessageHeaders headers = new MessageHeaders(
                Map.of(
                        RECEIVED_TOPIC, TOPIC,
                        RECEIVED_PARTITION, 0,
                        OFFSET, 0L));
        Object expected = "result";
        when(messageSendMessage.getHeaders()).thenReturn(headers);
        when(joinPoint.getArgs()).thenReturn(new Object[]{messageSendMessage});
        when(messageSendMessage.getPayload()).thenReturn(messageSend);
        when(messageSend.getMessageId()).thenReturn(MESSAGE_ID);
        when(joinPoint.proceed()).thenReturn(expected);

        // when
        Object actual = aspect.manageStructuredLogging(joinPoint);

        //then
        assertEquals(expected, actual);
        assertTrue(capture.getOut().contains("Processing message"));
        assertTrue(capture.getOut().contains("Processed message"));
        verifyInfoLogMap(capture);
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void shouldLogInfoWhenRetryableException(CapturedOutput capture) throws Throwable {
        // given
        MessageHeaders headers = new MessageHeaders(
                Map.of(
                        RECEIVED_TOPIC, TOPIC,
                        RECEIVED_PARTITION, 0,
                        OFFSET, 0L));

        when(joinPoint.getArgs()).thenReturn(new Object[]{messageSendMessage});
        when(messageSendMessage.getPayload()).thenReturn(messageSend);
        when(messageSendMessage.getHeaders()).thenReturn(headers);
        when(messageSend.getMessageId()).thenReturn(MESSAGE_ID);
        when(joinPoint.proceed()).thenThrow(RetryableException.class);

        // when
        Executable actual = () -> aspect.manageStructuredLogging(joinPoint);

        //then
        assertThrows(RetryableException.class, actual);
        assertTrue(capture.getOut().contains("Retryable exception thrown"));
        verifyInfoLogMap(capture);
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void shouldLogInfoWhenRetryableExceptionMaxAttempts(CapturedOutput capture) throws Throwable {
        // given
        MessageHeaders headers = new MessageHeaders(
                Map.of(
                        // attempt header returns a byte array not an integer
                        DEFAULT_HEADER_ATTEMPTS, ByteBuffer.allocate(4).putInt(5).array(),
                        RECEIVED_TOPIC, TOPIC,
                        RECEIVED_PARTITION, 0,
                        OFFSET, 0L));
        when(joinPoint.getArgs()).thenReturn(new Object[]{messageSendMessage});
        when(messageSendMessage.getPayload()).thenReturn(messageSend);
        when(messageSendMessage.getHeaders()).thenReturn(headers);
        when(messageSend.getMessageId()).thenReturn(MESSAGE_ID);
        when(joinPoint.proceed()).thenThrow(RetryableException.class);

        // when
        Executable actual = () -> aspect.manageStructuredLogging(joinPoint);

        //then
        assertThrows(RetryableException.class, actual);
        assertTrue(ERROR_EVENT_PATTERN.matcher(capture.getOut()).find());
        assertTrue(MAX_RETRY_ATTEMPTS_REACHED_PATTERN.matcher(capture.getOut()).find());
        assertTrue(REQUEST_ID_INITIALISED_PATTERN.matcher(capture.getOut()).find());
        assertTrue(RETRY_COUNT_FOUR_PATTERN.matcher(capture.getOut()).find());
        assertTrue(MAIN_TOPIC_PATTERN.matcher(capture.getOut()).find());
        assertTrue(PARTITION_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertTrue(OFFSET_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertTrue(CONTEXT_ID_PATTERN.matcher(capture.getOut()).find());
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void shouldLogInfoWhenInvalidPayload(CapturedOutput capture) {
        // given
        MessageHeaders headers = new MessageHeaders(
                Map.of(
                        RECEIVED_TOPIC, TOPIC,
                        RECEIVED_PARTITION, 0,
                        OFFSET, 0L));
        when(joinPoint.getArgs()).thenReturn(new Object[]{invalidMessage});
        when(invalidMessage.getPayload()).thenReturn("message payload");
        when(invalidMessage.getHeaders()).thenReturn(headers);

        // when
        Executable actual = () -> aspect.manageStructuredLogging(joinPoint);

        //then
        assertThrows(NonRetryableException.class, actual);
        assertTrue(ERROR_EVENT_PATTERN.matcher(capture.getOut()).find());
        assertTrue(INVALID_PAYLOAD_PATTERN.matcher(capture.getOut()).find());
        assertTrue(REQUEST_ID_INITIALISED_PATTERN.matcher(capture.getOut()).find());
        assertTrue(RETRY_COUNT_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertTrue(MAIN_TOPIC_PATTERN.matcher(capture.getOut()).find());
        assertTrue(PARTITION_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertTrue(OFFSET_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertFalse(capture.getOut().contains("transaction_id"));
    }

    private static void verifyInfoLogMap(CapturedOutput capture) {
        assertTrue(INFO_EVENT_PATTERN.matcher(capture.getOut()).find());
        assertTrue(REQUEST_ID_INITIALISED_PATTERN.matcher(capture.getOut()).find());
        assertTrue(RETRY_COUNT_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertTrue(MAIN_TOPIC_PATTERN.matcher(capture.getOut()).find());
        assertTrue(PARTITION_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertTrue(OFFSET_ZERO_PATTERN.matcher(capture.getOut()).find());
        assertTrue(CONTEXT_ID_PATTERN.matcher(capture.getOut()).find());
    }
}