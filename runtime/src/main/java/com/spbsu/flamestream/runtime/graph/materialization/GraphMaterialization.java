package com.spbsu.flamestream.runtime.graph.materialization;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public interface GraphMaterialization {
  Consumer<DataItem<?>> sourceInput();

  BiConsumer<GraphMaterializer.Destination, DataItem<?>> destinationInput();

  Consumer<GlobalTime> minTimeInput();

  Runnable commitInput();
}
