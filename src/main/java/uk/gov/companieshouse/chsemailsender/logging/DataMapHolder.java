package uk.gov.companieshouse.chsemailsender.logging;

import java.util.Map;
import uk.gov.companieshouse.logging.util.DataMap.Builder;

public class DataMapHolder {

    private static final String UNINITIALISED = "uninitialised";

    private static final ThreadLocal<Builder> DATAMAP_BUILDER
            = ThreadLocal.withInitial(() -> new Builder().requestId(UNINITIALISED));

    private static final ThreadLocal<String> REQUEST_ID
            = ThreadLocal.withInitial(() -> UNINITIALISED);

    public static void initialise(String requestId) {
        REQUEST_ID.set(requestId);
        DATAMAP_BUILDER.set(new Builder().requestId(requestId));
    }

    private DataMapHolder() {
    }

    public static void clear() {
        DATAMAP_BUILDER.remove();
        REQUEST_ID.remove();
    }

    public static Builder get() {
        return DATAMAP_BUILDER.get();
    }

    public static Map<String, Object> getLogMap() {
        return DATAMAP_BUILDER.get()
                .build()
                .getLogMap();
    }

    public static String getRequestId() {
        return REQUEST_ID.get();
    }
}