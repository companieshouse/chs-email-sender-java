package uk.gov.companieshouse.chsemailsender.logging;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.logging.util.DataMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
class DataMapHolderTest {

    private static final String REQUEST_ID = "requestId";

    @BeforeEach
    void setUp() {
        DataMapHolder.clear();
    }

    @Test
    void getLogMapWithExplicitRequestId() {
        DataMapHolder.initialise(REQUEST_ID);
        assertEquals(REQUEST_ID, DataMapHolder.getRequestId());
    }

    @Test
    void getLogMapWithDefaultRequestId() {
        assertEquals("uninitialised", DataMapHolder.getRequestId());
    }

    @Test
    void get() {
        DataMapHolder.initialise(REQUEST_ID);

        DataMap.Builder builder = DataMapHolder.get();
        DataMap dataMap = builder.build();
        assertEquals(REQUEST_ID, dataMap.getLogMap().get("request_id"));
    }

    @Test
    void clear() {
        DataMapHolder.clear();
        assertEquals("uninitialised", DataMapHolder.getRequestId());

        var logMap = DataMapHolder.getLogMap();
        assertTrue(logMap.containsKey("request_id"));
    }
}