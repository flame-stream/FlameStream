package com.spbsu.akka;

import akka.actor.Actor;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.system.RuntimeUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author vpdelta
 */
public class ActorInvokeDispatcher<A extends ActorAdapter> {
  private final List<Dispatcher> dispatchSequence = new ArrayList<>();

  public ActorInvokeDispatcher(final Actor actor, final AdapterProps[] props, final Action<Object> unhandledCallback) {
    this(actor, props, unhandledCallback, ActorMethod.class);
  }

  public ActorInvokeDispatcher(final Actor actor, final AdapterProps[] props, Action<Object> unhandledCallback, Class<? extends Annotation> annotation) {
    Action<Object> currentUnhandledCallback = unhandledCallback;
    for (AdapterProps p : props) {
      Class<?> clazz = null;
      try {
        // todo: quite dirty
        clazz = p.getAdapterClass();
        final Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        final A instance = (A) constructor.newInstance(Arrays.copyOf(p.getArgs(), constructor.getParameterCount()));
        final Dispatcher dispatcher = new Dispatcher(
          instance,
          new RuntimeUtils.InvokeDispatcher(clazz, currentUnhandledCallback, annotation)
        );
        instance.injectActor(actor);
        instance.injectUnhandled(currentUnhandledCallback);
        dispatchSequence.add(dispatcher);
        currentUnhandledCallback = dispatcher;
      } catch (Exception e) {
        throw new IllegalStateException("Unable to create actor for class: " + clazz, e);
      }
    }
  }

  public List<Dispatcher> getDispatchSequence() {
    return dispatchSequence;
  }

  public final void invoke(Object message) {
    dispatchSequence.get(dispatchSequence.size() - 1).invoke(message);
  }

  public class Dispatcher implements Action<Object> {
    private final A instance;
    private final RuntimeUtils.InvokeDispatcher invokeDispatcher;

    public Dispatcher(final A instance, final RuntimeUtils.InvokeDispatcher invokeDispatcher) {
      this.instance = instance;
      this.invokeDispatcher = invokeDispatcher;
    }

    @Override
    public void invoke(final Object o) {
      invokeDispatcher.invoke(instance, o);
    }

    public A getInstance() {
      return instance;
    }
  }
}
