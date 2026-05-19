package uk.gov.companieshouse.chsemailsender.serdes;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

public class KafkaPayloadSerialiser<T> implements Serializer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);

    private final Class<T> type;

    public KafkaPayloadSerialiser(Class<T> type) {
        this.type = type;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        DatumWriter<T> writer = getDatumWriter();
        try {
            writer.write(data, encoder);
        } catch (IOException ex) {
            final String msg = "Error serialising message payload";
            LOGGER.error(msg, ex, DataMapHolder.getLogMap());
            throw new NonRetryableException(msg, ex);
        }
        return outputStream.toByteArray();
    }

    public DatumWriter<T> getDatumWriter() {
        return new ReflectDatumWriter<>(type);
    }
}
