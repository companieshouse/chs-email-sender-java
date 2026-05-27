package uk.gov.companieshouse.chsemailsender.kafka;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.common.collect.Iterables;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Testcontainers
@WireMockTest(httpPort = 8889)
public abstract class AbstractKafkaIT {
    protected static final String MAIN_TOPIC = "email-send";
    protected static final String GROUP = "email-sender-consumer-group";
    protected static final String RETRY_TOPIC = "%s-%s-retry".formatted(MAIN_TOPIC, GROUP);
    protected static final String ERROR_TOPIC = "%s-%s-error".formatted(MAIN_TOPIC, GROUP);
    protected static final String INVALID_TOPIC = "%s-%s-invalid".formatted(MAIN_TOPIC, GROUP);

    @Container
    protected static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:latest");

    protected KafkaConsumer<String, byte[]> testConsumer = testConsumer(kafka.getBootstrapServers());

    protected KafkaProducer<String, byte[]> testProducer = testProducer(kafka.getBootstrapServers());

    @Autowired
    protected TestConsumerAspect testConsumerAspect;

    @BeforeEach
    void setup() {
        WireMock.reset();
        testConsumerAspect.resetLatch();
        testConsumer.subscribe(getSubscribedTopics());
        testConsumer.poll(Duration.ofMillis(1000));
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    protected List<String> getSubscribedTopics() {
        return List.of(MAIN_TOPIC, RETRY_TOPIC, ERROR_TOPIC, INVALID_TOPIC);
    }

    protected static int recordsPerTopic(ConsumerRecords<?, ?> records, String topic) {
        return Iterables.size(records.records(topic));
    }

    protected static <T> byte[] writePayloadToBytes(T data, Class<T> type) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
            DatumWriter<T> writer = new ReflectDatumWriter<>(type);
            writer.write(data, encoder);
            return outputStream.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static KafkaConsumer<String, byte[]> testConsumer(String bootstrapServers) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                        ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()),
                new StringDeserializer(), new ByteArrayDeserializer());
    }

    private static KafkaProducer<String, byte[]> testProducer(String bootstrapServers) {
        return new KafkaProducer<>(
                Map.of(
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                new StringSerializer(), new ByteArraySerializer());
    }
}