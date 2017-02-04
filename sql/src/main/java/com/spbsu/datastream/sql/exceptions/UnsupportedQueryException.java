package com.spbsu.datastream.sql.exceptions;

/**
 * Created by Artem on 23.11.2016.
 */
public class UnsupportedQueryException extends Exception {
  public UnsupportedQueryException() {
  }

  public UnsupportedQueryException(final String message) {
    super(message);
  }

  public UnsupportedQueryException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public UnsupportedQueryException(final Throwable cause) {
    super(cause);
  }
}
