package com.spbsu.datastream.sql.exceptions;

/**
 * Experts League
 * Created by solar on 27.10.16.
 */
public class TypeUnreachableException extends RuntimeException {
  public TypeUnreachableException() {
  }

  public TypeUnreachableException(final String message) {
    super(message);
  }

  public TypeUnreachableException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public TypeUnreachableException(final Throwable cause) {
    super(cause);
  }
}
