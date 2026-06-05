package uk.gov.companieshouse.chsemailsender.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class ApiClientConfig {

    @Bean
    public RestClient restClient(
            @Value("${internal.notification.api-url}") String notificationApiUrl,
            @Value("${internal.api-key}") String apiKey) {
        return RestClient.builder()
                .baseUrl(notificationApiUrl)
                .defaultHeader("Authorization", apiKey)
                .build();
    }
}
