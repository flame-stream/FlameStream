package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.front.FrontSubscription;

public interface SourceHandle<T> {
  void onSubscribe(FrontSubscription subscription);

  void onInput(DataItem<T> dataItem);

  void onWatermark(GlobalTime watermark);

  void onError(Throwable throwable);

  void onComplete();
}
