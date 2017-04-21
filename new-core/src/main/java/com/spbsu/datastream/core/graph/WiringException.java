package com.spbsu.datastream.core.graph;

public class WiringException extends RuntimeException {
  private static final long serialVersionUID = 1962808487048377310L;

  public WiringException() {
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
