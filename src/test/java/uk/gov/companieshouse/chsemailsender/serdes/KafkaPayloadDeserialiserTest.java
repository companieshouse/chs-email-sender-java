package uk.gov.companieshouse.chsemailsender.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createEmailSend;

import email.email_send;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import uk.gov.companieshouse.chsemailsender.exception.InvalidPayloadException;

class KafkaPayloadDeserialiserTest {

    @Test
    void testDeserialiseEmailSend() {
        // given
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {
            // when

            email_send emailSend = createEmailSend();
            email_send actual = deserialiser.deserialize("topic", writePayloadToBytes(emailSend, email_send.class));

            // then
            assertEquals(emailSend, actual);
        }
    }


    @Test
    void testDeserialiseThrowsInvalidPayloadExceptionWhenIOException() {
        // given
        byte[] payload = writePayloadToBytes("hello", String.class);
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {

            // when
            Executable actual = () -> deserialiser.deserialize("topic", payload);

            // then
            InvalidPayloadException exception = assertThrows(InvalidPayloadException.class, actual);
            assertInstanceOf(IOException.class, exception.getCause());
        }
    }

    @Test
    void testDeserialiseThrowsInvalidPayloadExceptionWhenAvroRuntimeException() {
        // given
        byte[] payload = "invalid".getBytes(StandardCharsets.UTF_8);
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {

            // when
            Executable actual = () -> deserialiser.deserialize("topic", payload);

            // then
            InvalidPayloadException exception = assertThrows(InvalidPayloadException.class, actual);
            assertInstanceOf(AvroRuntimeException.class, exception.getCause());
        }
    }

    @Test
    void testDeserialiseThrowsInvalidPayloadExceptionWhenDataIsNull() {
        // given
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {

            // when
            Executable actual = () -> deserialiser.deserialize("topic", null);

            // then
            InvalidPayloadException exception = assertThrows(InvalidPayloadException.class, actual);
            assertTrue(exception.getMessage().contains("null record value (tombstone)"));
        }
    }

    @Test
    void testDeserialiseDoesNotLeakPayloadContentsIntoExceptionMessage() {
        // given
        byte[] payload = "recipient@example.com Jane Doe 01234567".getBytes(StandardCharsets.UTF_8);
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {

            // when
            Executable actual = () -> deserialiser.deserialize("message-send", payload);

            // then
            InvalidPayloadException exception = assertThrows(InvalidPayloadException.class, actual);
            String message = exception.getMessage();
            assertFalse(message.contains("recipient@example.com"));
            assertFalse(message.contains("Jane Doe"));
            assertFalse(message.contains("01234567"));
        }
    }

    @Test
    void testDeserialiseProducesBoundedExceptionMessageForLargePayloads() {
        // given a payload far larger than the broker's default max.request.size
        byte[] payload = new byte[5 * 1024 * 1024];
        Arrays.fill(payload, (byte) 'a');
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {

            // when
            Executable actual = () -> deserialiser.deserialize("message-send", payload);

            // then the message must stay small enough to fit in a Kafka header
            InvalidPayloadException exception = assertThrows(InvalidPayloadException.class, actual);
            assertTrue(exception.getMessage().length() < 256,
                    "exception message must be bounded, was " + exception.getMessage().length());
        }
    }

    private static <T> byte[] writePayloadToBytes(T data, Class<T> type) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
            DatumWriter<T> writer = new ReflectDatumWriter<>(type);
            writer.write(data, encoder);
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}