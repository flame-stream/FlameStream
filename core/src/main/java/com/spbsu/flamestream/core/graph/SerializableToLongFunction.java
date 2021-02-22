package com.spbsu.flamestream.core.graph;

import java.io.Serializable;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public interface SerializableToLongFunction<T> extends Serializable, ToLongFunction<T> {}
