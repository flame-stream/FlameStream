package com.spbsu.akka;

/**
 * @author vpdelta
 */
public class AdapterProps {
  private final Class<? extends ActorAdapter> adapterClass;
  private final Object[] args;

  public static AdapterProps create(final Class<? extends ActorAdapter> adapterClass, final Object... args) {
    return new AdapterProps(adapterClass, args);
  }

  public AdapterProps(final Class<? extends ActorAdapter> adapterClass, final Object[] args) {
    this.adapterClass = adapterClass;
    this.args = args;
  }

  public Class<? extends ActorAdapter> getAdapterClass() {
    return adapterClass;
  }

  public Object[] getArgs() {
    return args;
  }
}
