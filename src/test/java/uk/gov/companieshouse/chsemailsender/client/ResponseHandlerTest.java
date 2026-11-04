package uk.gov.companieshouse.chsemailsender.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.security.cert.CertificateException;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
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
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;
import uk.gov.companieshouse.chsemailsender.exception.NonRetryableException;
import uk.gov.companieshouse.chsemailsender.exception.RetryableException;

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

    static Stream<IOException> transientTransportFailures() {
        return Stream.of(
                new ConnectException("Connection refused"),
                new UnknownHostException("notification-api"),
                new HttpTimeoutException("request timed out"));
    }

    @ParameterizedTest(name = "{0} is retryable")
    @MethodSource("transientTransportFailures")
    void shouldThrowRetryableForTransientTransportFailure(IOException cause) {
        // given
        ResourceAccessException ex = new ResourceAccessException("I/O error on POST request", cause);

        // when
        RetryableException actual = assertThrows(RetryableException.class,
                () -> responseHandler.handleTransport(ex, ERROR_MESSAGE));

        // then - the original exception is preserved as the cause for diagnosis
        assertEquals(ERROR_MESSAGE, actual.getMessage());
        assertSame(ex, actual.getCause());
    }

    @Test
    void shouldThrowNonRetryableForTlsHandshakeFailure() {
        // given - real JDK handshake failures carry a nested cause, e.g. CertificateException
        SSLHandshakeException ssl = new SSLHandshakeException("PKIX path building failed",
                new CertificateException("unable to find valid certification path"));
        ResourceAccessException ex = new ResourceAccessException("I/O error on POST request", ssl);

        // when
        NonRetryableException actual = assertThrows(NonRetryableException.class,
                () -> responseHandler.handleTransport(ex, ERROR_MESSAGE));

        // then
        assertEquals(ERROR_MESSAGE, actual.getMessage());
        assertSame(ex, actual.getCause());
    }

    @Test
    void shouldTreatTransportFailureWithoutCauseAsRetryable() {
        // given
        ResourceAccessException ex = new ResourceAccessException("I/O error on POST request");

        // when
        RetryableException actual = assertThrows(RetryableException.class,
                () -> responseHandler.handleTransport(ex, ERROR_MESSAGE));

        // then
        assertSame(ex, actual.getCause());
    }

}
