package com.spbsu.datastream.core.inference;

import java.util.function.Function;

/**
 * Created by marnikitta on 12/8/16.
 */
public interface PolymorphicMorphism extends Function<Morphism[], Morphism> {
  Morphism[] domain();

  Morphism codomain();
}
