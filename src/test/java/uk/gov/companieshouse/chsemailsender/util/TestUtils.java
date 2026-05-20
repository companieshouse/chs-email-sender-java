package uk.gov.companieshouse.chsemailsender.util;

import email.message_send;

public class TestUtils {
    public static message_send createMessageSend() {
        message_send messageSend = new message_send();
        messageSend.setMessageType("sanctions_roe_penalty_payment_received_email");
        messageSend.setCreatedAt("2026-05-19T07:11:35.461Z");
        messageSend.setData("{\"payable_resource\":{\"customer_code\":\"1234\",\"payable_ref\":\"1234\",\"etag\":\"1234\",\"created_by\":{\"email\":\"1234@gmail.com\",\"forename\":\"1234\",\"id\":\"1234\",\"surname\":\"1234\"},\"created_at\":\"2026-05-19T07:11:31.041Z\",\"links\":{\"self\":\"/company/1234/penalties/payable/1234\",\"payment\":\"/company/1234/penalties/payable/1234/payment\",\"resume_journey_uri\":\"https://test.com/pay-penalty/company/1234/penalty/1234/view-penalties\"}}}");
        messageSend.setAppId("penalty-payment-api.sanctions_roe_penalty_payment_received_email");
        messageSend.setMessageId("test-message-id");
        messageSend.setUserId("abc@gmail.com");
        return  messageSend;
    }
}
