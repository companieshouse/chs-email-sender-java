package uk.gov.companieshouse.chsemailsender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static final String NAMESPACE = "chs-email-sender-java";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
