package com.spbsu.flamestream.benchmarks.bl.inverted_index.utils;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class IndexLongUtil {
  private static final int pageIdBitLength = 24;
  private static final int positionBitLength = 20;
  private static final int versionBitLength = 8;
  private static final int rangeBitLength = 12;

  public static long createPagePosition(int pageId, int position, int pageVersion) {
    return createPagePosition(pageId, position, pageVersion, 0);
  }

  public static long createPagePosition(int pageId, int position, int pageVersion, int range) {
    return (((long) pageId) << (Long.SIZE - pageIdBitLength))
            | ((position & 0xffffffffL) << (Long.SIZE - pageIdBitLength - positionBitLength))
            | ((pageVersion & 0xffffffffL) << (Long.SIZE - pageIdBitLength - positionBitLength - versionBitLength))
            | ((range & 0xffffffffL));
  }

  public static long setRange(long l, int range) {
    final int pageId = pageId(l);
    final int position = position(l);
    final int version = version(l);
    return createPagePosition(pageId, position, version, range);
  }

  public static int pageId(long l) {
    return (int) (l >>> (positionBitLength + versionBitLength + rangeBitLength));
  }

  public static int position(long l) {
    return (int) ((l << pageIdBitLength) >>> (pageIdBitLength + versionBitLength + rangeBitLength));
  }

  public static int version(long l) {
    return (int) ((l << (pageIdBitLength + positionBitLength)) >>> (pageIdBitLength + positionBitLength + rangeBitLength));
  }

  public static int range(long l) {
    return (int) ((l << (pageIdBitLength + positionBitLength + versionBitLength)) >>> (pageIdBitLength + positionBitLength + versionBitLength));
  }
}
