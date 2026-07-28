package uk.gov.companieshouse.chsemailsender.service;

import email.email_send;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.chsemailsender.client.NotificationApiClient;

@Service
public class SendEmailService {

    private final NotificationApiClient notificationApiClient;

    public SendEmailService(NotificationApiClient notificationApiClient) {
        this.notificationApiClient = notificationApiClient;
    }

    public void processMessage(email_send emailSend) {
        notificationApiClient.postEmail(emailSend.getMessageType(), emailSend.getAppId(), emailSend.getData());
    }
}
