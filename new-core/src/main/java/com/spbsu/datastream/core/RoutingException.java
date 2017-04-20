package com.spbsu.datastream.core;

public class RoutingException extends RuntimeException {
  private static final long serialVersionUID = 5189602567459523932L;

  public RoutingException() {
    super();
  }

  public RoutingException(final String message) {
    super(message);
  }

  public RoutingException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public RoutingException(final Throwable cause) {
    super(cause);
  }

  protected RoutingException(final String message, final Throwable cause, final boolean enableSuppression,
                             final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
