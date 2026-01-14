package com.github.dimitryivaniuta.kraftdemo.reliability.exception;

/**
 * Retryable error: e.g. temporary DB/network/service issue.
 * Spring Kafka will retry the record; on exhaustion we store as poison.
 */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }

    public TransientProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
