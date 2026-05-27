package uk.gov.companieshouse.chsemailsender.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationApiClientTest {

    private static final String EMAIL_URI = "/email";
    private static final String INVALID_URI = "invalid-uri";
    private static final String POST_FAILED_MSG = "POST failed for %s data, status code: [%d]";
    private static final String INVALID_URI_MSG = "POST /email failed due to invalid URI";
    private static final String TEMPLATE_NAME = "template-name";
    private static final String APP_ID = "app-id";
    private static final String DATA = "{\"field\":\"value\"}";

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private InternalApiClient internalApiClient;

    @Mock
    private ApiErrorResponseException apiErrorResponseException;

    @InjectMocks
    private NotificationApiClient notificationApiClient;

    @BeforeEach
    void setUp() {
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
    }

    @Test
    void postEmailShouldSendEmailWhenApiCallSucceeds() throws ApiErrorResponseException, URIValidationException {
        callPostEmail();

        verify(internalApiClientSupplier, times(1)).get();
        verify(internalApiClient.chsEmailHandler().postChsEmail(EMAIL_URI, TEMPLATE_NAME, APP_ID, DATA),
                times(1)).execute();
    }

    @Test
    void postEmailShouldThrowNonRetryableExceptionWhenApiReturnsBadRequest() throws ApiErrorResponseException, URIValidationException {
        when(apiErrorResponseException.getStatusCode()).thenReturn(400);
        stubExecuteThrows(apiErrorResponseException);

        NonRetryableException actual = assertThrows(NonRetryableException.class,
                this::callPostEmail);

        assertEquals(POST_FAILED_MSG.formatted(DATA, 400), actual.getMessage());
        assertInstanceOf(ApiErrorResponseException.class, actual.getCause());
    }

    @Test
    void postEmailShouldThrowNonRetryableExceptionWhenApiReturnsConflict() throws ApiErrorResponseException, URIValidationException {
        when(apiErrorResponseException.getStatusCode()).thenReturn(409);
        stubExecuteThrows(apiErrorResponseException);

        NonRetryableException actual = assertThrows(NonRetryableException.class,
                this::callPostEmail);

        assertEquals(POST_FAILED_MSG.formatted(DATA, 409), actual.getMessage());
        assertInstanceOf(ApiErrorResponseException.class, actual.getCause());
    }

    @Test
    void postEmailShouldThrowRetryableExceptionWhenApiReturnsNonMappedStatus() throws ApiErrorResponseException, URIValidationException {
        when(apiErrorResponseException.getStatusCode()).thenReturn(500);
        stubExecuteThrows(apiErrorResponseException);

        RetryableException actual = assertThrows(RetryableException.class,
                this::callPostEmail);

        assertEquals(POST_FAILED_MSG.formatted(DATA, 500), actual.getMessage());
        assertInstanceOf(ApiErrorResponseException.class, actual.getCause());
    }

    @Test
    void postEmailShouldThrowNonRetryableExceptionWhenUriIsInvalid() {
        URIValidationException uriValidationException = new URIValidationException(INVALID_URI);
        when(internalApiClient.chsEmailHandler().postChsEmail(EMAIL_URI, TEMPLATE_NAME, APP_ID, DATA))
                .thenAnswer(invocation -> {
                    throw uriValidationException;
                });

        NonRetryableException actual = assertThrows(NonRetryableException.class,
                this::callPostEmail);

        assertEquals(INVALID_URI_MSG, actual.getMessage());
        assertInstanceOf(URIValidationException.class, actual.getCause());
    }

    private void callPostEmail() {
        notificationApiClient.postEmail(TEMPLATE_NAME, APP_ID, DATA);
    }

    private void stubExecuteThrows(ApiErrorResponseException exception)
            throws ApiErrorResponseException, URIValidationException {
        when(internalApiClient.chsEmailHandler().postChsEmail(EMAIL_URI, TEMPLATE_NAME, APP_ID, DATA).execute())
                .thenThrow(exception);
    }
}
