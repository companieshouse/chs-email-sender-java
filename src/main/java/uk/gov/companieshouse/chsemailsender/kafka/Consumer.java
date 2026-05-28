package uk.gov.companieshouse.chsemailsender.kafka;

import email.email_send;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;
import uk.gov.companieshouse.chsemailsender.service.SendEmailService;

@Component
public class Consumer {

    private final SendEmailService sendEmailService;
    private final MessageFlags messageFlags;

    public Consumer(SendEmailService sendEmailService, MessageFlags messageFlags) {
        this.sendEmailService = sendEmailService;
        this.messageFlags = messageFlags;
    }

    @KafkaListener(
            id = "${kafka.consumer.topic}-consumer",
            containerFactory = "listenerContainerFactory",
            topics = "${kafka.consumer.topic}",
            groupId = "${kafka.consumer.group}"
    )
    public void consume(Message<email_send> message) {
        try {
            sendEmailService.processMessage(message.getPayload());
        } catch (RetryableException ex) {
            messageFlags.setRetryable(true);
            throw ex;
        }
    }
}
