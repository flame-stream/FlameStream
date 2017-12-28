package com.spbsu.flamestream.example.bl.index.utils;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public final class IndexItemInLong {
  private static final int PAGE_ID_BIT_LENGTH = 24;
  private static final int POSITION_BIT_LENGTH = 20;
  private static final int VERSION_BIT_LENGTH = 8;
  private static final int RANGE_BIT_LENGTH = 12;
  public static final long LOW_QUADWORD_MASK = 0xffffffffL;

  private IndexItemInLong() {}

  public static long createPagePosition(int pageId, int position, int pageVersion) {
    return createPagePosition(pageId, position, pageVersion, 0);
  }

  public static long createPagePosition(int pageId, int position, int pageVersion, int range) {
    //noinspection OverlyComplexBooleanExpression,UnnecessaryParentheses
    return (((long) pageId) << (Long.SIZE - PAGE_ID_BIT_LENGTH)) | ((position & LOW_QUADWORD_MASK) << (Long.SIZE
            - PAGE_ID_BIT_LENGTH
            - POSITION_BIT_LENGTH)) | ((pageVersion & LOW_QUADWORD_MASK) << (Long.SIZE - PAGE_ID_BIT_LENGTH
            - POSITION_BIT_LENGTH - VERSION_BIT_LENGTH)) | ((range & LOW_QUADWORD_MASK));
  }

  public static long setRange(long l, int range) {
    final int pageId = pageId(l);
    final int position = position(l);
    final int version = version(l);
    return createPagePosition(pageId, position, version, range);
  }

  public static int pageId(long l) {
    //noinspection UnnecessaryParentheses,NumericCastThatLosesPrecision
    return (int) (l >>> (POSITION_BIT_LENGTH + VERSION_BIT_LENGTH + RANGE_BIT_LENGTH));
  }

  public static int position(long l) {
    //noinspection NumericCastThatLosesPrecision,UnnecessaryParentheses
    return (int) ((l << PAGE_ID_BIT_LENGTH) >>> (PAGE_ID_BIT_LENGTH + VERSION_BIT_LENGTH + RANGE_BIT_LENGTH));
  }

  public static int version(long l) {
    //noinspection UnnecessaryParentheses,NumericCastThatLosesPrecision
    return (int) ((l << (PAGE_ID_BIT_LENGTH + POSITION_BIT_LENGTH)) >>> (PAGE_ID_BIT_LENGTH + POSITION_BIT_LENGTH
            + RANGE_BIT_LENGTH));
  }

  public static int range(long l) {
    //noinspection NumericCastThatLosesPrecision,UnnecessaryParentheses
    return (int) ((l << (PAGE_ID_BIT_LENGTH + POSITION_BIT_LENGTH + VERSION_BIT_LENGTH)) >>> (PAGE_ID_BIT_LENGTH
            + POSITION_BIT_LENGTH + VERSION_BIT_LENGTH));
  }
}
