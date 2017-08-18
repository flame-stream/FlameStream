package com.spbsu.datastream.benchmarks.bl.inverted_index.ops;

import com.spbsu.datastream.benchmarks.bl.inverted_index.ops.InvertedIndexState;
import com.spbsu.datastream.benchmarks.bl.inverted_index.utils.IndexLongUtil;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class InvertedIndexStateTest {
  @Test
  public void storageInsertAndFindTest() {
    //Arrange
    final int elementsToAddNum = 100000;
    final TLongList expectedValues = new TLongArrayList();
    final InvertedIndexState tree = new InvertedIndexState();
    final int pageIds[] = IntStream.range(0, elementsToAddNum + 1).toArray();
    shuffleArray(pageIds);

    for (int pageId : pageIds) {
      final long value = IndexLongUtil.createPagePosition(pageId, 0, 0);
      final int newPosition = ThreadLocalRandom.current().nextInt(1,101);
      final int newRange = ThreadLocalRandom.current().nextInt(1,101);
      //Act
      tree.insert(value);

      //Assert
      Assert.assertEquals(value, tree.tryToFindAndUpdate(value, newPosition, newRange));
      expectedValues.add(IndexLongUtil.createPagePosition(IndexLongUtil.pageId(value), newPosition, IndexLongUtil.version(value), newRange));
    }

    expectedValues.sort();
    Assert.assertEquals(expectedValues.toArray(), tree.toList().toArray());
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
