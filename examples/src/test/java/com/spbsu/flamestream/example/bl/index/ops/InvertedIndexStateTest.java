package com.spbsu.flamestream.example.bl.index.ops;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
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
public class InvertedIndexStateTest extends FlameStreamSuite {
  private static void shuffleArray(int[] ar) {
    final Random rnd = ThreadLocalRandom.current();
    for (int i = ar.length - 1; i > 0; i--) {
      final int index = rnd.nextInt(i + 1);
      final int a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }

  @Test
  public void storageInsertAndFindTest() {
    //Arrange
    final int elementsToAddNum = 100000;
    final TLongList expectedValues = new TLongArrayList();
    final InvertedIndexState tree = new InvertedIndexState();
    final int[] pageIds = IntStream.range(0, elementsToAddNum + 1).toArray();
    shuffleArray(pageIds);

    for (int pageId : pageIds) {
      final long value = IndexItemInLong.createPagePosition(pageId, 0, 0);
      final int newPosition = ThreadLocalRandom.current().nextInt(1, 101);
      final int newRange = ThreadLocalRandom.current().nextInt(1, 101);
      //Act
      tree.insert(value);

      //Assert
      Assert.assertEquals(value, tree.tryToFindAndUpdate(value, newPosition, newRange));
      expectedValues.add(IndexItemInLong.createPagePosition(
              IndexItemInLong.pageId(value),
              newPosition,
              IndexItemInLong.version(value),
              newRange
      ));
    }

    expectedValues.sort();
    Assert.assertEquals(expectedValues.toArray(), tree.toList().toArray());
  }
}
