package uk.gov.companieshouse.chsemailsender;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static uk.gov.companieshouse.logging.util.LogContextProperties.REQUEST_ID;

@AutoConfigureMockMvc
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApplicationIT {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void shouldReturn200FromGetHealthEndpoint() throws Exception {
        this.mockMvc.perform(get("/healthcheck")
                        .header(REQUEST_ID.value(), "request_id"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json("{\"status\":\"UP\"}"));    }
}
