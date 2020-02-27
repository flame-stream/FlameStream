package com.spbsu.flamestream.core.data.meta;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.stream.IntStream;

public class LabelsPresence {
  public static LabelsPresence EMPTY = new LabelsPresence();

  private final int[] all;

  public LabelsPresence() { this.all = new int[0]; }

  public LabelsPresence(int[] all) { this.all = all.clone(); }

  @NotNull
  public IntStream stream() {
    return IntStream.of(all);
  }
}
