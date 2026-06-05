package uk.gov.companieshouse.chsemailsender.client;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;
import uk.gov.companieshouse.chsemailsender.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import static uk.gov.companieshouse.chsemailsender.Application.NAMESPACE;

@Component
public class ResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);

    public void handle(RestClientResponseException ex, String exceptionMessage) {
        HttpStatus httpStatus = HttpStatus.valueOf(ex.getStatusCode().value());
        LOGGER.error(exceptionMessage, ex, DataMapHolder.getLogMap());
        if (HttpStatus.CONFLICT.equals(httpStatus) || HttpStatus.BAD_REQUEST.equals(httpStatus)) {
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
