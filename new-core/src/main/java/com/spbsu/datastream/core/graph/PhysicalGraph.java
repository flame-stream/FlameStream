package com.spbsu.datastream.core.graph;

/**
 * Created by marnikitta on 2/8/17.
 */

/**
 * Represents node that can be materialized
 * <p>
 * e.g.
 * Source is physical.
 * Rehash is logical element. In fact it should be preprocessed into HashwiseSplit that is Physical
 */
public interface PhysicalGraph extends AtomicGraph {
}
