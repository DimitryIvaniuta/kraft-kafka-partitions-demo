package com.github.dimitryivaniuta.kraftdemo.reliability.exception;

/**
 * Non-retryable error: the message payload is permanently invalid (schema, validation, etc.).
 * We treat it as "poison" and store it, then advance the consumer offset.
 */
public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException(String message) {
        super(message);
    }

    public PoisonMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
