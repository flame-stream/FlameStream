package com.spbsu.datastream.example.invertedindex.models.long_containers;

import org.jetbrains.annotations.NotNull;

/**
 * User: Artem
 * Date: 15.03.2017
 * Time: 17:46
 */
public class IndexLongContainer implements LongContainer, Comparable<IndexLongContainer> {
  private static final int pageIdBitLength = 22;
  private static final int versionBitLength = 10;
  private static final int positionBitLength = 20;
  private static final int rangeBitLength = 12;

  private long value;

  public IndexLongContainer(IndexLongContainer indexLongContainer, int range) {
    this(indexLongContainer.pageId(), indexLongContainer.version(), indexLongContainer.position(), range);
  }

  public IndexLongContainer(PageLongContainer[] pageLongContainers) {
    this(pageLongContainers[0].pageId(), pageLongContainers[0].version(), pageLongContainers[0].position(), pageLongContainers.length);
  }

  public IndexLongContainer(int pageId, int pageVersion, int position) {
    this(pageId, pageVersion, position, 0);
  }

  public IndexLongContainer(int pageId, int pageVersion, int position, int range) {
    value = (((long) pageId) << (Long.SIZE - pageIdBitLength))
            | ((pageVersion & 0xffffffffL) << (Long.SIZE - pageIdBitLength - versionBitLength))
            | ((position & 0xffffffffL) << (Long.SIZE - pageIdBitLength - versionBitLength - positionBitLength))
            | ((range & 0xffffffffL));
  }

  public long value() {
    return value;
  }

  public int pageId() {
    return (int) (value >>> (positionBitLength + versionBitLength + rangeBitLength));
  }

  public int version() {
    return (int) ((value << pageIdBitLength) >>> (pageIdBitLength + positionBitLength + rangeBitLength));
  }

  public int position() {
    return (int) ((value << (pageIdBitLength + versionBitLength)) >>> (pageIdBitLength + versionBitLength + rangeBitLength));
  }

  public int range() {
    return (int) ((value << (pageIdBitLength + positionBitLength + versionBitLength)) >>> (pageIdBitLength + positionBitLength + versionBitLength));
  }

  public void setPosition(int position) {
    final int range = range();
    value = (value >>> rangeBitLength + positionBitLength) << (rangeBitLength + positionBitLength)
            | ((position & 0xffffffffL) << (Long.SIZE - pageIdBitLength - versionBitLength - positionBitLength))
            | ((range & 0xffffffffL));
  }

  public void setRange(int range) {
    value = ((value >>> rangeBitLength) << rangeBitLength) | (range & 0xffffffffL);
  }

  @Override
  public int compareTo(@NotNull IndexLongContainer o) {
    return Long.compare(value, o.value);
  }
}
