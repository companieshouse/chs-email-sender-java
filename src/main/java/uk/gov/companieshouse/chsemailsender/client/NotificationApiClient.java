package uk.gov.companieshouse.chsemailsender.client;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import uk.gov.companieshouse.chsemailsender.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

@Component
public class NotificationApiClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);
    private static final String EMAIL_URI = "/email";
    private static final String HEADER_APP_ID = "appID";
    private static final String HEADER_TEMPLATE_NAME = "templateName";

    private final RestClient restClient;
    private final ResponseHandler responseHandler;

    public NotificationApiClient(RestClient restClient, ResponseHandler responseHandler) {
        this.restClient = restClient;
        this.responseHandler = responseHandler;
    }

    public void postEmail(String templateName, String appId, String data) {
        try {
            HttpStatusCode statusCode = restClient.post()
                    .uri(EMAIL_URI)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header(HEADER_APP_ID, appId)
                    .header(HEADER_TEMPLATE_NAME, templateName)
                    .body(data)
                    .retrieve()
                    .toBodilessEntity()
                    .getStatusCode();
            LOGGER.info("POST email succeeded for appId: %s with status code: %d"
                    .formatted(appId, statusCode.value()), DataMapHolder.getLogMap());
        } catch (RestClientResponseException ex) {
            String exceptionMessage = "POST failed for appId: %s with  status code: [%d]"
                    .formatted(appId, ex.getStatusCode().value());
            responseHandler.handle(ex, exceptionMessage);
        } catch (Exception ex) {
            String exceptionMessage = "POST %s failed due to connection error".formatted(EMAIL_URI);
            responseHandler.handle(ex, exceptionMessage);
        }
    }
}