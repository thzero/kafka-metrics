package com.example.kafkametrics.logging;

import org.slf4j.MDC;

public final class MdcContext {

    public static final String INTERACTION_ID = "interactionId";
    public static final String MESSAGE_ID = "messageId";

    private MdcContext() {
    }

    public static void set(String interactionId, String messageId) {
        if (interactionId != null) {
            MDC.put(INTERACTION_ID, interactionId);
        }
        if (messageId != null) {
            MDC.put(MESSAGE_ID, messageId);
        }
    }

    public static void clear() {
        MDC.remove(INTERACTION_ID);
        MDC.remove(MESSAGE_ID);
    }
}
