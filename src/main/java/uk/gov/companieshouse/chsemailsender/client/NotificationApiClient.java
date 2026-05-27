package uk.gov.companieshouse.chsemailsender.client;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;
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

    public NotificationApiClient(Supplier<InternalApiClient> internalApiClientSupplier) {
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    public void postEmail(String templateName, String appId, String data) {

        InternalApiClient client = internalApiClientSupplier.get();
        try {
            ApiResponse<Void> response = client.chsEmailHandler().postChsEmail(EMAIL_URI, templateName, appId, data)
                    .execute();
            LOGGER.info("POST email succeeded for data: %s and code %s".formatted(data, response.getStatusCode()), DataMapHolder.getLogMap());
        } catch (ApiErrorResponseException ex) {
            final int statusCode = ex.getStatusCode();
            String errorMsg = "POST failed for %s data, status code: [%d]".formatted(data, statusCode);
            LOGGER.error(errorMsg, ex, DataMapHolder.getLogMap());

            if (HttpStatus.BAD_REQUEST.value() == statusCode || HttpStatus.CONFLICT.value() == statusCode) {
                throw new NonRetryableException(errorMsg, ex);
            } else {
                throw new RetryableException(errorMsg, ex);
            }
        } catch (URIValidationException ex) {
            String errorMsg = "POST %s failed due to invalid URI".formatted(EMAIL_URI);
            LOGGER.error(errorMsg, ex, DataMapHolder.getLogMap());
            throw new NonRetryableException(errorMsg, ex);
        }
    }
}