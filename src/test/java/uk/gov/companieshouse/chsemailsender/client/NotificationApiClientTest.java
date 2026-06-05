package uk.gov.companieshouse.chsemailsender.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationApiClientTest {

    private static final String EMAIL_URI = "/email";
    private static final String POST_FAILED_MSG = "POST failed for %s data, status code: [%d]";
    private static final String TEMPLATE_NAME = "template-name";
    private static final String APP_ID = "app-id";
    private static final String DATA = "{\"field\":\"value\"}";

    @Mock(answer = RETURNS_DEEP_STUBS)
    private RestClient restClient;

    @Mock
    private ResponseHandler responseHandler;

    @InjectMocks
    private NotificationApiClient notificationApiClient;

    private void stubChain(Object returnValue) {
        var stub = when(restClient.post()
                .uri(EMAIL_URI)
                .contentType(any())
                .header(anyString(), anyString())
                .header(anyString(), anyString())
                .body(DATA)
                .retrieve()
                .toBodilessEntity());
        if (returnValue instanceof Throwable t) {
            stub.thenThrow(t);
        } else {
            stub.thenReturn((ResponseEntity<Void>) returnValue);
        }
    }

    @Test
    void postEmailShouldExecuteApiCallSuccessfully() {
        stubChain(ResponseEntity.ok().build());

        notificationApiClient.postEmail(TEMPLATE_NAME, APP_ID, DATA);

        verify(responseHandler, never()).handle(any(RestClientResponseException.class), anyString());
        verify(responseHandler, never()).handle(any(Exception.class), anyString());
    }

    @Test
    void postEmailShouldDelegateToResponseHandlerWhenApiReturnsBadRequest() {
        RestClientResponseException ex = new HttpClientErrorException(HttpStatus.BAD_REQUEST);
        stubChain(ex);

        notificationApiClient.postEmail(TEMPLATE_NAME, APP_ID, DATA);

        verify(responseHandler, times(1))
                .handle(ex, POST_FAILED_MSG.formatted(DATA, HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    void postEmailShouldDelegateToResponseHandlerWhenApiReturnsServerError() {
        RestClientResponseException ex = new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
        stubChain(ex);

        notificationApiClient.postEmail(TEMPLATE_NAME, APP_ID, DATA);

        verify(responseHandler, times(1))
                .handle(ex, POST_FAILED_MSG.formatted(DATA, HttpStatus.INTERNAL_SERVER_ERROR.value()));
    }

    @Test
    void postEmailShouldDelegateToResponseHandlerWhenConnectionFails() {
        Exception connectionError = new RuntimeException("connection refused");
        stubChain(connectionError);

        notificationApiClient.postEmail(TEMPLATE_NAME, APP_ID, DATA);

        verify(responseHandler, times(1))
                .handle(connectionError, "POST %s failed due to connection error".formatted(EMAIL_URI));
    }
}
