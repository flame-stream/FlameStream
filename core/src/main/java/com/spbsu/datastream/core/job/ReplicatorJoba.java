package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.job.control.Control;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ReplicatorJoba extends Joba.AbstractJoba {
  public ReplicatorJoba(Sink... lists) {
    this(Arrays.asList(lists));
  }

  public ReplicatorJoba(Collection<Sink> sinks) {
    super(null);
    this.sinks.addAll(sinks);
  }

  public void add(Sink sink) {
    sinks.add(sink);
  }

  private List<Sink> sinks = new ArrayList<>();

  @Override
  public void accept(DataItem item) {
    for (Sink sink : sinks) {
      sink.accept(item);
    }
  }

  @Override
  public void accept(Control control) {
    for (Sink sink : sinks) {
      sink.accept(control);
    }
  }
}
