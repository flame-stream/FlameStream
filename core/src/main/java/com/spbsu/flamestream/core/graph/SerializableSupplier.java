package com.spbsu.flamestream.core.graph;

import java.io.Serializable;
import java.util.function.Supplier;

public interface SerializableSupplier<T> extends Serializable, Supplier<T> {
}
