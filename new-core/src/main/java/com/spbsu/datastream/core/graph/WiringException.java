package com.spbsu.datastream.core.graph;

public class WiringException extends RuntimeException {
  private static final long serialVersionUID = 1962808487048377310L;

  public WiringException() {
  }

  public WiringException(String message) {
    super(message);
  }

  public WiringException(String message, Throwable cause) {
    super(message, cause);
  }

  public WiringException(Throwable cause) {
    super(cause);
  }

  protected WiringException(String message, Throwable cause, boolean enableSuppression,
                            boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
