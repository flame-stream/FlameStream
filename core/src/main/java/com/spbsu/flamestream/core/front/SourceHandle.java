package com.spbsu.flamestream.core.front;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

public interface SourceHandle<T> extends Consumer<DataItem<T>> {
  void onSubscribe(FrontSubscription subscription);

  @Override
  void accept(DataItem<T> dataItem);

  void submit(GlobalTime watermark);

  void onError(Throwable throwable);

  void onComplete();
}
