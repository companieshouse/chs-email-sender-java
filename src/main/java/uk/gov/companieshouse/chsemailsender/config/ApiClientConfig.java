package uk.gov.companieshouse.chsemailsender.config;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class ApiClientConfig {

    @Bean
    public RestClient restClient(
            @Value("${internal.notification.api-url}") String notificationApiUrl,
            @Value("${internal.api-key}") String apiKey,
            @Value("${internal.notification.connect-timeout-ms}") long connectTimeoutMs,
            @Value("${internal.notification.read-timeout-ms}") long readTimeoutMs) {

        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "internal.api-key is missing or blank; set the CHS_INTERNAL_API_KEY environment variable");
        }

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();

        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofMillis(readTimeoutMs));

        return RestClient.builder()
                .requestFactory(requestFactory)
                .baseUrl(notificationApiUrl)
                .defaultHeader("Authorization", apiKey)
                .build();
    }
}
