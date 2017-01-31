package com.spbsu.datastream.example.invertedindex;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class PageInfo implements Comparable<PageInfo> {
  @JsonProperty
  private final int pageId;
  @JsonProperty
  private final double freq;

  public PageInfo(int pageId, double freq) {
    this.pageId = pageId;
    this.freq = freq;
  }

  public int pageId() {
    return pageId;
  }

  public double freq() {
    return freq;
  }

  @Override
  public int compareTo(@NotNull PageInfo pageInfo) {
    return (int) Math.signum(freq - pageInfo.freq);
  }
}
