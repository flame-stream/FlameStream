package com.spbsu.flamestream.example.index;

import com.spbsu.flamestream.example.AbstractExampleTest;
import com.spbsu.flamestream.example.FlameStreamExample;
import org.testng.annotations.Test;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class InvertedIndexTest extends AbstractExampleTest {

  @Test
  public void testIndexWithSmallDump() {
    test(InvertedIndexCheckers.CHECK_INDEX_WITH_SMALL_DUMP, 2, 2, 20, 25);
  }

  @Test
  public void testIndexAndRankingStorageWithSmallDump() {
    test(InvertedIndexCheckers.CHECK_INDEX_AND_RANKING_STORAGE_WITH_SMALL_DUMP, 4, 4, 60, 25);
  }

  @Test
  public void testIndexWithRanking() {
    test(InvertedIndexCheckers.CHECK_INDEX_WITH_RANKING, 4, 4, 90, 25);
  }

  @Override
  protected FlameStreamExample example() {
    return FlameStreamExample.INVERTED_INDEX;
  }
}
