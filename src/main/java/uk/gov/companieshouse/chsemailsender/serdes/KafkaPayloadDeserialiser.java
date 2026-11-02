package uk.gov.companieshouse.chsemailsender.serdes;

import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import uk.gov.companieshouse.chsemailsender.exception.InvalidPayloadException;
import uk.gov.companieshouse.chsemailsender.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

public class KafkaPayloadDeserialiser<T> implements Deserializer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);

    /**
     * Number of hex characters of the SHA-256 digest retained in diagnostics. 16 hex characters
     * (64 bits) is ample to distinguish records within an invalid topic while keeping log lines and
     * Kafka headers a fixed, bounded size.
     */
    private static final int FINGERPRINT_LENGTH = 16;

    private final Class<T> type;

    public KafkaPayloadDeserialiser(Class<T> type) {
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            throw new InvalidPayloadException(
                    "Invalid payload: null record value (tombstone) received on topic: [%s]".formatted(topic));
        }
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            DatumReader<T> reader = new ReflectDatumReader<>(type);
            return reader.read(null, decoder);
        } catch (IOException | AvroRuntimeException ex) {
            // The payload itself is deliberately never included. It carries recipient addresses,
            // names and template variables, and would otherwise be copied verbatim into the log
            // sink and into the EXCEPTION_MESSAGE/EXCEPTION_STACKTRACE headers of the invalid-topic
            // record. The undecodable bytes remain available on the invalid topic, which is access
            // controlled; the fingerprint below is enough to correlate a log line with that record.
            String description = describe(topic, data);
            LOGGER.error("Error deserialising message payload: %s".formatted(description), ex,
                    DataMapHolder.getLogMap());
            throw new InvalidPayloadException("Invalid payload was provided: %s".formatted(description), ex);
        }
    }

    /**
     * Builds a bounded, PII-free description of a payload that failed to deserialise.
     */
    private static String describe(String topic, byte[] data) {
        return "topic: [%s], size: [%d] bytes, sha256: [%s]".formatted(topic, data.length, fingerprint(data));
    }

    private static String fingerprint(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
            return HexFormat.of().formatHex(digest).substring(0, FINGERPRINT_LENGTH);
        } catch (NoSuchAlgorithmException ex) {
            // SHA-256 is mandated by the JDK specification, so this is unreachable in practice.
            return "unavailable";
        }
    }
}
