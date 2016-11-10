package com.spbsu.akka;

/**
 * @author vpdelta
 */
public class Envelope {
  private final Object[] messages;

  public static Envelope of(final Object first, final Object... other) {
    final Object[] messages = new Object[1 + other.length];
    messages[0] = first;
    System.arraycopy(other, 0, messages, 1, other.length);
    return new Envelope(messages);
  }

  public Envelope(final Object[] messages) {
    this.messages = messages;
  }

  public Object[] getMessages() {
    return messages;
  }
}
