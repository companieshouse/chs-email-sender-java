package uk.gov.companieshouse.chsemailsender.config;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class ApiClientConfigTest {

    private static final String API_URL = "http://localhost:8889";
    private static final long CONNECT_TIMEOUT_MS = 3000L;
    private static final long READ_TIMEOUT_MS = 10000L;

    private final ApiClientConfig apiClientConfig = new ApiClientConfig();

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "\t"})
    void restClientShouldFailFastWhenApiKeyIsMissingOrBlank(String apiKey) {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> apiClientConfig.restClient(API_URL, apiKey, CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS));

        assertTrue(ex.getMessage().contains("CHS_INTERNAL_API_KEY"));
    }
}

