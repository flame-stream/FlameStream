package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.DataItem;

/**
 * User: Artem
 * Date: 14.11.2017
 */
public class SimpleSource extends AbstractSource {

  @Override
  public void onNext(DataItem<?> item, SourceHandle handle) {
    handle.push(outPort(), item);
    handle.ack(item.xor(), item.meta().globalTime());
    handle.accept(item.meta().globalTime());
  }
}
