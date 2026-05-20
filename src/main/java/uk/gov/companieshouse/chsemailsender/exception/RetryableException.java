package uk.gov.companieshouse.chsemailsender.exception;

import uk.gov.companieshouse.api.error.ApiErrorResponseException;

public class RetryableException extends RuntimeException {

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String formatted, ApiErrorResponseException ex) {
        super(formatted, ex);
    }
}