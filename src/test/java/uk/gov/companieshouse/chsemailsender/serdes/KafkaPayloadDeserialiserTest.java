package uk.gov.companieshouse.chsemailsender.serdes;

import email.email_send;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import uk.gov.companieshouse.chsemailsender.exception.InvalidPayloadException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createEmailSend;

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
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {

            // when
            Executable actual = () -> deserialiser.deserialize("topic", writePayloadToBytes("hello", String.class));

            // then
            InvalidPayloadException exception = assertThrows(InvalidPayloadException.class, actual);
            assertInstanceOf(IOException.class, exception.getCause());
        }
    }

    @Test
    void testDeserialiseThrowsInvalidPayloadExceptionWhenAvroRuntimeException() {
        // given
        try (KafkaPayloadDeserialiser<email_send> deserialiser = new KafkaPayloadDeserialiser<>(email_send.class)) {

            // when
            Executable actual = () -> deserialiser.deserialize("topic", "invalid".getBytes(StandardCharsets.UTF_8));

            // then
            InvalidPayloadException exception = assertThrows(InvalidPayloadException.class, actual);
            assertInstanceOf(AvroRuntimeException.class, exception.getCause());
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