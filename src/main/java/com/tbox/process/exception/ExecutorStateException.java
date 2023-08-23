package com.tbox.process.exception;

public class ExecutorStateException extends ExecutorException {
    public ExecutorStateException() {
    }

    public ExecutorStateException(String message) {
        super(message);
    }

    public ExecutorStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExecutorStateException(Throwable cause) {
        super(cause);
    }

    public ExecutorStateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
