package com.spbsu.datastream.core.materializer;

import org.jetbrains.annotations.NonNls;

/**
 * Created by marnikitta on 2/7/17.
 */
public class MaterializationException extends RuntimeException {
  public MaterializationException() {
    super();
  }

  public MaterializationException(@NonNls final String message) {
    super(message);
  }

  public MaterializationException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public MaterializationException(final Throwable cause) {
    super(cause);
  }

  protected MaterializationException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
