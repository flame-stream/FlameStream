package com.spbsu.datastream.core.graph;

import com.google.common.collect.Sets;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Created by marnikitta on 2/7/17.
 */
public class ComposedGraphTest {

  @Test
  public void correctWiring() {
    final Processor pr1 = new Identity();
    final Processor pr2 = new Identity();

    final OutPort out = pr1.outPorts().stream().findAny().get();
    final InPort in = pr2.inPorts().stream().findAny().get();

    final ComposedGraph graph = new ComposedGraph(Sets.newHashSet(pr1, pr2), Collections.singletonMap(out, in));

    Assert.assertEquals(graph.outPorts(), pr2.outPorts());
    Assert.assertEquals(graph.inPorts(), pr1.inPorts());
    Assert.assertEquals(graph.downstreams(), Collections.singletonMap(out, in));
    Assert.assertEquals(graph.upstreams(), Collections.singletonMap(in, out));
  }

  @Test(expectedExceptions = WiringException.class)
  public void occupiedPortWiring() {
    final Processor pr1 = new Identity();
    final Processor pr2 = new Identity();

    final OutPort out = pr1.outPorts().stream().findAny().get();
    final InPort in = pr2.inPorts().stream().findAny().get();

    final ComposedGraph graph = new ComposedGraph(Sets.newHashSet(pr1, pr2), Collections.singletonMap(out, in));

    final ComposedGraph graph2 = new ComposedGraph(Collections.singleton(graph), Collections.singletonMap(out, in));
  }

  @Test(expectedExceptions = WiringException.class)
  public void foreignPortWiring() {
    final Processor pr1 = new Identity();

    final ComposedGraph graph = new ComposedGraph(pr1, new OutPort(), new InPort());
  }
}