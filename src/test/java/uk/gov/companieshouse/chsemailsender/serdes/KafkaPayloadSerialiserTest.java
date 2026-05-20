package uk.gov.companieshouse.chsemailsender.serdes;

import email.message_send;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static uk.gov.companieshouse.chsemailsender.util.TestUtils.createMessageSend;

@ExtendWith(MockitoExtension.class)
class KafkaPayloadSerialiserTest {

    @Mock
    private DatumWriter<message_send> writer;

    @Test
    void testSerialiseMessageSend() {
        // given
        try (KafkaPayloadSerialiser<message_send> serialiser = new KafkaPayloadSerialiser<>(message_send.class)) {
           message_send messageSend=createMessageSend();
            // when
            byte[] actual = serialiser.serialize("topic", messageSend);

            // then
            assertTrue(actual.length > 0);
        }
    }

    @Test
    void testSerialiseThrowsNonRetryableExceptionWhenIOException() throws IOException {
        // given
        KafkaPayloadSerialiser<message_send> serialiser = spy(new KafkaPayloadSerialiser<>(message_send.class));
        doReturn(writer).when(serialiser).getDatumWriter();
        doThrow(IOException.class).when(writer).write(any(), any());

        // when
        Executable actual = () -> serialiser.serialize("topic", new message_send());

        // then
        NonRetryableException exception = assertThrows(NonRetryableException.class, actual);
        assertInstanceOf(IOException.class, exception.getCause());
    }
}
