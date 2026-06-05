package uk.gov.companieshouse.chsemailsender.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientResponseException;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class ResponseHandlerTest {

    private static final String ERROR_MESSAGE = "something went wrong";

    @InjectMocks
    private ResponseHandler responseHandler;

    static Stream<Arguments> apiErrorStatusCodes() {
        return Stream.of(
                Arguments.of(new HttpClientErrorException(HttpStatus.BAD_REQUEST), NonRetryableException.class),
                Arguments.of(new HttpClientErrorException(HttpStatus.CONFLICT), NonRetryableException.class),
                Arguments.of(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR), RetryableException.class),
                Arguments.of(new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE), RetryableException.class)
        );
    }

    @ParameterizedTest(name = "status {0} throws {1}")
    @MethodSource("apiErrorStatusCodes")
    void shouldThrowCorrectExceptionForHttpStatusCode(RestClientResponseException ex,
            Class<? extends RuntimeException> expectedException) {
        RuntimeException actual = assertThrows(expectedException,
                () -> responseHandler.handle(ex, ERROR_MESSAGE));

        assertEquals(ERROR_MESSAGE, actual.getMessage());
        assertInstanceOf(RestClientResponseException.class, actual.getCause());
    }

    @Test
    void shouldThrowNonRetryableExceptionForGenericConnectionError() {
        Exception connectionError = new RuntimeException("connection refused");

        NonRetryableException actual = assertThrows(NonRetryableException.class,
                () -> responseHandler.handle(connectionError, ERROR_MESSAGE));

        assertEquals(ERROR_MESSAGE, actual.getMessage());
        assertInstanceOf(RuntimeException.class, actual.getCause());
    }
}
