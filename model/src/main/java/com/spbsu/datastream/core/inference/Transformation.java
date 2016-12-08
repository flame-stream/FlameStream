package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.type.TypeTemplate;

import java.util.function.Function;

/**
 * Created by marnikitta on 12/8/16.
 */
public interface Transformation extends Function<TypeTemplate[], TypeTemplate> {
  TypeTemplate[] domain();

  TypeTemplate codomain();
}
