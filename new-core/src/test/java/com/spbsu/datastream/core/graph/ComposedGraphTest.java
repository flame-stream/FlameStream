package com.spbsu.datastream.core.graph;

import com.google.common.collect.Sets;
import com.spbsu.datastream.core.HashFunction;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;

@SuppressWarnings("unchecked")
public class ComposedGraphTest {

  @Test
  public void correctWiring() {
    final Processor pr1 = new Identity();
    final Processor pr2 = new Identity();

    final ComposedGraph<Graph> composed = (ComposedGraph<Graph>) pr1.compose(pr2);
    Assert.assertEquals(composed.subGraphs(), Sets.newHashSet(pr1, pr2));
    Assert.assertEquals(composed.downstreams(), Collections.emptyMap());

    final ComposedGraph<Graph> fused = (ComposedGraph<Graph>) composed.wire(pr1.outPort(), pr2.inPort());

    Assert.assertEquals(new HashSet<>(fused.outPorts()), new HashSet<>(pr2.outPorts()));
    Assert.assertEquals(new HashSet<>(fused.inPorts()), new HashSet<>(pr1.inPorts()));

    Assert.assertEquals(fused.downstreams(), Collections.singletonMap(pr1.outPort(), pr2.inPort()));
    Assert.assertEquals(fused.subGraphs(), Sets.newHashSet(composed));
  }

  @Test(expectedExceptions = WiringException.class)
  public void occupiedPortWiring() {
    final Processor pr1 = new Identity();
    final Processor pr2 = new Identity();

    final ComposedGraph<Graph> graph = (ComposedGraph<Graph>) pr1.fuse(pr1, pr1.outPort(), pr2.inPort());

    final ComposedGraph<Graph> wrongGraph = (ComposedGraph<Graph>) pr1.fuse(graph, pr1.outPort(), pr2.inPort());
  }

  @Test(expectedExceptions = WiringException.class)
  public void foreignPortWiring() {
    final Processor pr1 = new Identity();

    final ComposedGraphImpl graph = new ComposedGraphImpl(pr1, new OutPort(), new InPort(HashFunction.OBJECT_HASH));
  }
}