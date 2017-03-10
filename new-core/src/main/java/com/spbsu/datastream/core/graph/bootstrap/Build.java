package com.spbsu.datastream.core.graph.bootstrap;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.graph.*;
import com.spbsu.datastream.core.graph.ops.StatelessFilter;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Build extends Processor<Set<DataType>, TheGraph> {

  private ShardMappedGraph mapper(final InetSocketAddress address) {
    final StatelessFilter<String, String> filter = new StatelessFilter<>(s -> String.format("%s {%s was here}", s, address));
    return new ShardMappedGraph(FlatGraph.flattened(filter), address);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    final List<InetSocketAddress> workers = handle.workers();
    final List<ShardMappedGraph> mappers = workers.stream().map(this::mapper).collect(Collectors.toList());
    // TODO: 3/10/17
  }

  @Override
  public Graph deepCopy() {
    return new Build();
  }
}
