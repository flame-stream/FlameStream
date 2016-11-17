package com.spbsu.datastream.core;

/**
 * Created by Artem on 15.11.2016.
 */
public class TypeNotSupportedException extends Exception {
  public TypeNotSupportedException() {
  }

  public TypeNotSupportedException(final String message) {
    super(message);
  }

  public TypeNotSupportedException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public TypeNotSupportedException(final Throwable cause) {
    super(cause);
  }
}
