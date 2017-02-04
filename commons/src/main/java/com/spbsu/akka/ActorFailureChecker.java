package com.spbsu.akka;

import scala.util.Failure;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author vpdelta
 */
public class ActorFailureChecker {
  private static final Logger log = Logger.getLogger(ActorFailureChecker.class.getName());

  public static boolean checkIfFailure(final Class actorClass, final String actorName, final Object message) {
    if (message instanceof Failure) {
      final Failure failure = (Failure) message;
      //noinspection ThrowableResultOfMethodCallIgnored
      if (failure.exception() != null)
        log.log(Level.WARNING, "Failure in" + actorClass + ":" + actorName, failure.exception());
      else
        log.log(Level.WARNING, failure.toString());
      return true;
    }
    return false;
  }
}
