package com.spbsu.experiments.inverted_index.common_bl.utils;

import com.spbsu.experiments.inverted_index.common_bl.utils.InvertedIndexStorage;
import com.spbsu.experiments.inverted_index.common_bl.utils.PagePositionLong;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * User: Artem
 * Date: 19.03.2017
 * Time: 14:44
 */
public class InvertedIndexStorageTest {
  @Test
  public void storageInsertAndFindTest() {
    //Arrange
    final int elementsToAddNum = 100000;
    final TLongList expectedValues = new TLongArrayList();
    final InvertedIndexStorage tree = new InvertedIndexStorage();
    final int pageIds[] = IntStream.range(0, elementsToAddNum + 1).toArray();
    shuffleArray(pageIds);

    for (int pageId : pageIds) {
      final long value = PagePositionLong.createPagePosition(pageId, 0, 0);
      final int newPosition = ThreadLocalRandom.current().nextInt(1,101);
      final int newRange = ThreadLocalRandom.current().nextInt(1,101);
      //Act
      tree.insert(value);

      //Assert
      Assert.assertEquals(value, tree.tryToFindAndUpdate(value, newPosition, newRange));
      expectedValues.add(PagePositionLong.createPagePosition(PagePositionLong.pageId(value), newPosition, PagePositionLong.version(value), newRange));
    }

    expectedValues.sort();
    Assert.assertArrayEquals(expectedValues.toArray(), tree.toList().toArray());
  }

  private static void shuffleArray(int[] ar) {
    final Random rnd = ThreadLocalRandom.current();
    for (int i = ar.length - 1; i > 0; i--) {
      final int index = rnd.nextInt(i + 1);
      final int a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }
}
