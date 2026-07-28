package uk.gov.companieshouse.chsemailsender.client;

import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;
import uk.gov.companieshouse.chsemailsender.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

@Component
public class ResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);

    private static final int BAD_REQUEST = 400;
    private static final int CONFLICT = 409;

    public void handle(RestClientResponseException ex, String exceptionMessage) {
        int statusCode = ex.getStatusCode().value();
        LOGGER.error(exceptionMessage, ex, DataMapHolder.getLogMap());
        if (statusCode == BAD_REQUEST || statusCode == CONFLICT) {
            throw new NonRetryableException(exceptionMessage, ex);
        } else {
            throw new RetryableException(exceptionMessage, ex);
        }
    }

    public void handle(Exception ex, String exceptionMessage) {
        LOGGER.error(exceptionMessage, ex, DataMapHolder.getLogMap());
        throw new NonRetryableException(exceptionMessage, ex);
    }

}
