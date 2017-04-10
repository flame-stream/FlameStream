package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

public class PreSinkMetaFilter<T> extends Processor<T, PreSinkMetaElement<T>> {
  public PreSinkMetaFilter(final HashFunction<? super T> hash) {
    super(hash);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    final DataItem<?> newItem = new PayloadDataItem<>(
            handle.copyAndAppendLocal(item.meta(), false),
            new PreSinkMetaElement<>(item.payload(), item.meta().rootHash()));
    prePush(newItem, handle);
    handle.push(outPort(), newItem);
    ack(item, handle);
  }
}
