package uk.gov.companieshouse.chsemailsender.config;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.http.ApiKeyHttpClient;

import java.util.function.Supplier;

@Configuration
public class ApiClientConfig {

    @Bean
    Supplier<InternalApiClient> internalApiClientSupplier(
            @Value("${internal.api-key}") String apiKey,
            @Value("${internal.notification.api-url}") String apiUrl) {
        return () -> {
            InternalApiClient internalApiClient = new InternalApiClient(new ApiKeyHttpClient(apiKey));
            internalApiClient.setBasePath(apiUrl);
            return internalApiClient;
        };
    }
}
