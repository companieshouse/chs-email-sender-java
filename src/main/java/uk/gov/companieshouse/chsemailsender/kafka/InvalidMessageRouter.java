package uk.gov.companieshouse.chsemailsender.kafka;

import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_MESSAGE;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TOPIC;
import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.logging.util.DataMap;

public class InvalidMessageRouter implements ProducerInterceptor<String, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);
    private static final int UNKNOWN_PARTITION = -1;
    private static final long UNKNOWN_OFFSET = -1L;

    private MessageFlags messageFlags;
    private String invalidTopic;

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> producerRecord) {
        if (messageFlags.isRetryable()) {
            messageFlags.destroy();
            return producerRecord;
        }

        String originalTopic = header(producerRecord, ORIGINAL_TOPIC)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElse(producerRecord.topic());
        int partition = header(producerRecord, ORIGINAL_PARTITION)
                .filter(value -> value.length == Integer.BYTES)
                .map(value -> ByteBuffer.wrap(value).getInt())
                .orElse(UNKNOWN_PARTITION);
        long offset = header(producerRecord, ORIGINAL_OFFSET)
                .filter(value -> value.length == Long.BYTES)
                .map(value -> ByteBuffer.wrap(value).getLong())
                .orElse(UNKNOWN_OFFSET);
        String exception = header(producerRecord, EXCEPTION_MESSAGE)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElse("unknown");

        LOGGER.error("""
                        Republishing record to topic: [%s] \
                        From: original topic: [%s], partition: [%s], offset: [%s], exception: [%s]\
                        """.formatted(invalidTopic, originalTopic, partition, offset, exception),
                buildLogMap(originalTopic, partition, offset));

        return new ProducerRecord<>(invalidTopic, null, producerRecord.key(), producerRecord.value(),
                producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            return;
        }
        // Runs on the producer I/O thread, so the log context is built explicitly rather than read
        // from DataMapHolder, which is thread-local to the consumer thread.
        String topic = metadata == null ? "unknown" : metadata.topic();
        LOGGER.error("Failed to publish record to topic: [%s]".formatted(topic), exception,
                new DataMap.Builder().topic(topic).build().getLogMap());
    }

    @Override
    public void close() {
        // N/A
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.messageFlags = (MessageFlags) configs.get("message-flags");
        this.invalidTopic = (String) configs.get("invalid-topic");
        if (messageFlags == null || invalidTopic == null) {
            throw new IllegalStateException(
                    "InvalidMessageRouter requires both 'message-flags' and 'invalid-topic' producer properties");
        }
    }

    private static Map<String, Object> buildLogMap(String originalTopic, int partition, long offset) {
        return new DataMap.Builder()
                .requestId("%s-%d-%d".formatted(originalTopic, partition, offset))
                .topic(originalTopic)
                .partition(partition)
                .offset(offset)
                .build()
                .getLogMap();
    }

    private static Optional<byte[]> header(ProducerRecord<String, Object> producerRecord, String name) {
        return Optional.ofNullable(producerRecord.headers().lastHeader(name)).map(Header::value);
    }
}
