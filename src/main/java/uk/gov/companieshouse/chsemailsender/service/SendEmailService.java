package uk.gov.companieshouse.chsemailsender.service;

import email.email_send;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.chsemailsender.client.NotificationApiClient;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

@Service
public class SendEmailService {

    private static final Logger logger = LoggerFactory.getLogger(NAMESPACE);

    private final NotificationApiClient notificationApiClient;

    public SendEmailService(NotificationApiClient notificationApiClient) {
        this.notificationApiClient = notificationApiClient;
    }

    public void processMessage(email_send emailSend) {
        notificationApiClient.postEmail(emailSend.getMessageType(), emailSend.getAppId(), emailSend.getData());
    }
}
