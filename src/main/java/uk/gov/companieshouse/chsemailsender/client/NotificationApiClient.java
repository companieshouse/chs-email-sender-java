package uk.gov.companieshouse.chsemailsender.client;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.chsemailsender.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import java.util.function.Supplier;

import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

@Component
public class NotificationApiClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);
    private static final String EMAIL_URI = "/email";
    private final Supplier<InternalApiClient> internalApiClientSupplier;
    private final ResponseHandler responseHandler;

    public NotificationApiClient(Supplier<InternalApiClient> internalApiClientSupplier, ResponseHandler responseHandler) {
        this.internalApiClientSupplier = internalApiClientSupplier;
        this.responseHandler = responseHandler;
    }

    public void postEmail(String templateName, String appId, String data) {
        InternalApiClient client = internalApiClientSupplier.get();
        try {
            ApiResponse<Void> response = client.chsEmailHandler().postChsEmail(EMAIL_URI, templateName, appId, data)
                    .execute();
            int statusCode = response.getStatusCode();
            LOGGER.info("POST email succeeded for appId: %s with status code %s".formatted(appId, statusCode), DataMapHolder.getLogMap());
        } catch (ApiErrorResponseException ex) {
            String exceptionMessage = "POST failed for %s data, status code: [%d]".formatted(data, ex.getStatusCode());
            responseHandler.handle(ex, exceptionMessage);
        } catch (URIValidationException ex) {
            String exceptionMessage = "POST failed due to invalid URI";
            responseHandler.handle(ex, exceptionMessage);
        }
    }
}