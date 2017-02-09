package com.spbsu.datastream.core;

import java.util.function.Supplier;

/**
 * Created by Artem on 01.12.2016.
 */
@FunctionalInterface
public interface Condition<T extends RunningCondition> extends Supplier<T> {
}