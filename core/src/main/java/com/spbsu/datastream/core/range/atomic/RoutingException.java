package com.spbsu.datastream.core.range.atomic;

public class RoutingException extends RuntimeException {
  private static final long serialVersionUID = 5189602567459523932L;

  public RoutingException() {
  }

  public RoutingException(String message) {
    super(message);
  }

  public RoutingException(String message, Throwable cause) {
    super(message, cause);
  }

  public RoutingException(Throwable cause) {
    super(cause);
  }

  protected RoutingException(String message, Throwable cause, boolean enableSuppression,
                             boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
