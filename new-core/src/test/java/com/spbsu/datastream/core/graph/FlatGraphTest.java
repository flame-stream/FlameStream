package com.spbsu.datastream.core.graph;

import com.google.common.collect.Sets;
import com.spbsu.datastream.core.graph.ops.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

public class FlatGraphTest {

  @SuppressWarnings("unchecked")
  @Test
  public void atomicFlattening() {
    final Identity identity = new Identity();
    final FlatGraph flatGraph = FlatGraph.flattened(identity);

    Assert.assertEquals(flatGraph.downstreams(), Collections.emptyMap());
    Assert.assertEquals(new HashSet<>(flatGraph.inPorts()), new HashSet<>(identity.inPorts()));
    Assert.assertEquals(new HashSet<>(flatGraph.outPorts()), new HashSet<>(identity.outPorts()));
    Assert.assertEquals(flatGraph.subGraphs(), Collections.singleton(identity));
  }

  @Test
  public void simpleLinear() {
    final StatelessFilter<Integer, Integer> filter = new StatelessFilter<>(i -> i);
    final Identity identity = new Identity();
    final StatelessFilter<Integer, Integer> filter1 = new StatelessFilter<>(i -> i);
    final Graph fused = filter.fuse(identity,
            filter.outPort(),
            identity.inPort())
            .fuse(filter1, identity.outPort(), filter1.inPort());

    final FlatGraph flatGraph = FlatGraph.flattened(fused);

    Assert.assertEquals(flatGraph.subGraphs(), Sets.newHashSet(filter, filter1, identity));
    Assert.assertEquals(new HashSet<>(flatGraph.inPorts()), new HashSet<>(filter.inPorts()));
    Assert.assertEquals(new HashSet<>(flatGraph.outPorts()), new HashSet<>(filter1.outPorts()));

    final Map<OutPort, InPort> expectedDownstreams = new HashMap<>();
    expectedDownstreams.put(filter.outPort(), identity.inPort());
    expectedDownstreams.put(identity.outPort(), filter1.inPort());
    Assert.assertEquals(flatGraph.downstreams(), expectedDownstreams);
  }

  @Test
  public void complex() {
    final Source<Integer> source = new SpliteratorSource<>(Stream.of(1).spliterator());
    final Broadcast<Integer> broadcast = new Broadcast<>(2);
    final StatelessFilter<Integer, Integer> f0 = new StatelessFilter<>(i -> i);
    final StatelessFilter<Integer, Integer> f1 = new StatelessFilter<>(i -> i);
    final Merge<Integer> merge = new Merge<>(2);
    final Sink<Integer> sink = new ConsumerSink<>(System.out::println);

    final Graph superGraph = source.fuse(broadcast, source.outPort(), broadcast.inPort()).fuse(f0, broadcast.outPorts().get(0), f0.inPort())
            .fuse(merge, f0.outPort(), merge.inPorts().get(0)).fuse(sink, merge.outPort(), sink.inPort())
            .compose(f1)
            .wire(broadcast.outPorts().get(1), f1.inPort()).wire(f1.outPort(), merge.inPorts().get(1));

    final FlatGraph flattened = FlatGraph.flattened(superGraph);

    Assert.assertEquals(flattened.inPorts(), Collections.emptySet());
    Assert.assertEquals(flattened.outPorts(), Collections.emptySet());
    Assert.assertEquals(flattened.subGraphs(), Sets.newHashSet(source, broadcast, f0, f1, merge, sink));

    final Map<OutPort, InPort> downstreams = new HashMap<>();
    downstreams.put(source.outPort(), broadcast.inPort());
    downstreams.put(broadcast.outPorts().get(0), f0.inPort());
    downstreams.put(broadcast.outPorts().get(1), f1.inPort());
    downstreams.put(f0.outPort(), merge.inPorts().get(0));
    downstreams.put(f1.outPort(), merge.inPorts().get(1));
    downstreams.put(merge.outPort(), sink.inPort());

    Assert.assertEquals(flattened.downstreams(), downstreams);
  }
}