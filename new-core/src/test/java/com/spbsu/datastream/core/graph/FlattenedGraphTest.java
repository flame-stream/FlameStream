package com.spbsu.datastream.core.graph;

import com.google.common.collect.Sets;
import com.spbsu.datastream.core.graph.impl.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 2/7/17.
 */
public class FlattenedGraphTest {

  @Test
  public void atomicFlattening() {
    final Identity identity = new Identity();
    final FlattenedGraph flattenedGraph = FlattenedGraph.flattened(identity);

    Assert.assertEquals(flattenedGraph.upstreams(), Collections.emptyMap());
    Assert.assertEquals(flattenedGraph.downstreams(), Collections.emptyMap());
    Assert.assertEquals(flattenedGraph.inPorts(), identity.inPorts());
    Assert.assertEquals(flattenedGraph.outPorts(), identity.outPorts());
    Assert.assertEquals(flattenedGraph.subGraphs(), Collections.singleton(identity));
  }

  @Test
  public void simpleLinear() {
    final StatelessFilter<Integer, Integer> filter = new StatelessFilter<>(i -> i * 2);
    final Identity identity = new Identity();
    final StatelessFilter<Integer, String> filter1 = new StatelessFilter<>(Object::toString);
    final Graph fused = filter.fuse(identity,
            filter.outPort(),
            identity.inPort())
            .fuse(filter1, identity.outPort(), filter1.inPort());

    final FlattenedGraph flattenedGraph = FlattenedGraph.flattened(fused);

    Assert.assertEquals(flattenedGraph.subGraphs(), Sets.newHashSet(filter, filter1, identity));
    Assert.assertEquals(flattenedGraph.inPorts(), filter.inPorts());
    Assert.assertEquals(flattenedGraph.outPorts(), filter1.outPorts());

    final Map<OutPort, InPort> expectedDownstreams = new HashMap<>();
    expectedDownstreams.put(filter.outPort(), identity.inPort());
    expectedDownstreams.put(identity.outPort(), filter1.inPort());
    Assert.assertEquals(flattenedGraph.downstreams(), expectedDownstreams);

    final Map<InPort, OutPort> expectedUpstreams = new HashMap<>();
    expectedUpstreams.put(filter1.inPort(), identity.outPort());
    expectedUpstreams.put(identity.inPort(), filter.outPort());
    Assert.assertEquals(flattenedGraph.upstreams(), expectedUpstreams);
  }

  @Test
  public void complex() {
    final Source<Integer> source = new SpliteratorSource<>(Stream.generate(() -> 1).spliterator());
    final Broadcast broadcast = new Broadcast(2);
    final List<OutPort> bOuts = new ArrayList<>(broadcast.outPorts());
    final StatelessFilter<Integer, Integer> f0 = new StatelessFilter<>(i -> i + 1);
    final StatelessFilter<Integer, Integer> f1 = new StatelessFilter<>(i -> i + 2);
    final Merge merge = new Merge(2);
    final List<InPort> mIns = new ArrayList<>(merge.inPorts());
    final Sink sink = new ConsumerSink<>(System.out::println);

    final Graph superGraph = source.fuse(broadcast, source.outPort(), broadcast.inPort()).fuse(f0, bOuts.get(0), f0.inPort())
            .fuse(merge, f0.outPort(), mIns.get(0)).fuse(sink, merge.outPort(), sink.inPort())
            .compose(f1)
            .wire(bOuts.get(1), f1.inPort()).wire(f1.outPort(), mIns.get(1));

    final FlattenedGraph flattened = FlattenedGraph.flattened(superGraph);

    Assert.assertEquals(flattened.inPorts(), Collections.emptySet());
    Assert.assertEquals(flattened.outPorts(), Collections.emptySet());
    Assert.assertEquals(flattened.subGraphs(), Sets.newHashSet(source, broadcast, f0, f1, merge, sink));

    final Map<OutPort, InPort> dowstreams = new HashMap<>();
    dowstreams.put(source.outPort(), broadcast.inPort());
    dowstreams.put(bOuts.get(0), f0.inPort());
    dowstreams.put(bOuts.get(1), f1.inPort());
    dowstreams.put(f0.outPort(), mIns.get(0));
    dowstreams.put(f1.outPort(), mIns.get(1));
    dowstreams.put(merge.outPort(), sink.inPort());

    Assert.assertEquals(flattened.downstreams(), dowstreams);
    Assert.assertEquals(flattened.upstreams(), dowstreams.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));

    System.out.println(superGraph);
  }
}