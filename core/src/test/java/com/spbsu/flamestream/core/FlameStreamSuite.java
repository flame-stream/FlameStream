package com.spbsu.flamestream.core;

import org.testng.annotations.BeforeMethod;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public abstract class FlameStreamSuite {
  @BeforeMethod
  public void beforeMethod(Method method) {
    System.out.println("Method name:" + method.getName());
  }

  // FIXME: 12/11/17 consume Stream rather than collection
  public <T> Consumer<T> randomConsumer(Collection<Consumer<T>> consumers) {
    final List<Consumer<T>> li = new ArrayList<>(consumers);
    return o -> li.get(ThreadLocalRandom.current().nextInt(li.size())).accept(o);
  }
}
