package com.spbsu.datastream.core.graph;

public class WiringException extends RuntimeException {
  public WiringException() {
    super();
  }

  public WiringException(final String message) {
    super(message);
  }

  public WiringException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public WiringException(final Throwable cause) {
    super(cause);
  }

  protected WiringException(final String message, final Throwable cause, final boolean enableSuppression,
                            final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
