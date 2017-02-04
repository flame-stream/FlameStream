package com.spbsu.datastream.sql.exceptions;

/**
 * Created by Artem on 23.11.2016.
 */
public class InvalidQueryException extends Exception {
  public InvalidQueryException() {
  }

  public InvalidQueryException(final String message) {
    super(message);
  }

  public InvalidQueryException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public InvalidQueryException(final Throwable cause) {
    super(cause);
  }
}
