package uk.gov.companieshouse.chsemailsender.util;

import email.email_send;

public class TestUtils {
    public static email_send createEmailSend() {
        email_send emailSend = new email_send();
        emailSend.setMessageType("sanctions_roe_penalty_payment_received_email");
        emailSend.setCreatedAt("2026-05-19T07:11:35.461Z");
        emailSend.setData("{\"payable_resource\":{\"customer_code\":\"1234\",\"payable_ref\":\"1234\",\"etag\":\"1234\",\"created_by\":{\"email\":\"1234@gmail.com\",\"forename\":\"1234\",\"id\":\"1234\",\"surname\":\"1234\"},\"created_at\":\"2026-05-19T07:11:31.041Z\",\"links\":{\"self\":\"/company/1234/penalties/payable/1234\",\"payment\":\"/company/1234/penalties/payable/1234/payment\",\"resume_journey_uri\":\"https://test.com/pay-penalty/company/1234/penalty/1234/view-penalties\"}}}");
        emailSend.setAppId("penalty-payment-api.sanctions_roe_penalty_payment_received_email");
        emailSend.setMessageId("test-message-id");
        emailSend.setEmailAddress("abc@gmail.com");
        return emailSend;
    }
}
