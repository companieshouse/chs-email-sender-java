package uk.gov.companieshouse.chsemailsender.logging;

import email.email_send;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS;
import static org.springframework.kafka.support.KafkaHeaders.OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC;
import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

@Component
@Aspect
class LoggingKafkaListenerAspect {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);

    private final int maxAttempts;

    LoggingKafkaListenerAspect(@Value("${kafka.consumer.retry.max-attempts}") int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    @Around("@annotation(org.springframework.kafka.annotation.KafkaListener)")
    public Object manageStructuredLogging(ProceedingJoinPoint joinPoint) throws Throwable {
        int retryCount = 0;
        try {
            Message<?> message = (Message<?>) joinPoint.getArgs()[0];
            MessageHeaders headers = message.getHeaders();

            String topic = (String) headers.get(RECEIVED_TOPIC);
            Integer partition = (Integer) headers.get(RECEIVED_PARTITION);
            Long offset = (Long) headers.get(OFFSET);

            DataMapHolder.initialise("%s-%d-%d".formatted(topic, partition, offset));

            retryCount = Optional.ofNullable(headers.get(DEFAULT_HEADER_ATTEMPTS))
                    .map(attempts -> ByteBuffer.wrap((byte[]) attempts).getInt())
                    .orElse(1) - 1;

            DataMapHolder.get()
                    .retryCount(retryCount)
                    .topic(topic)
                    .partition(partition)
                    .offset(offset);

            // Add to log map last to ensure other headers are logged in the event of invalid payload
            DataMapHolder.get().contextId(extractMessageId(message.getPayload()));

            LOGGER.info("Processing message", DataMapHolder.getLogMap());

            Object result = joinPoint.proceed();

            LOGGER.info("Processed message", DataMapHolder.getLogMap());

            return result;
        } catch (RetryableException ex) {
            // maxAttempts includes first attempt which is not a retry
            if (retryCount >= maxAttempts - 1) {
                LOGGER.error("Max retry attempts reached", ex, DataMapHolder.getLogMap());
            } else {
                LOGGER.error("Retryable exception thrown", ex, DataMapHolder.getLogMap());
            }
            throw ex;
        } catch (Exception ex) {
            LOGGER.error("Exception thrown", ex, DataMapHolder.getLogMap());
            throw ex;
        } finally {
            DataMapHolder.clear();
        }
    }

    private String extractMessageId(Object payload) {
        if (payload instanceof email_send emailSend) {
            return emailSend.getMessageId();
        }
        String errorMessage = "Invalid payload type, payload: [%s]".formatted(payload.toString());
        LOGGER.error(errorMessage, DataMapHolder.getLogMap());
        throw new NonRetryableException(errorMessage);
    }
}