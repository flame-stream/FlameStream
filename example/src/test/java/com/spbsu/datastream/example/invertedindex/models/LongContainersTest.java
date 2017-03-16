package com.spbsu.datastream.example.invertedindex.models;

import com.spbsu.datastream.example.invertedindex.models.long_containers.IndexLongContainer;
import com.spbsu.datastream.example.invertedindex.models.long_containers.PageLongContainer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

/**
 * User: Artem
 * Date: 15.03.2017
 * Time: 18:26
 */
public class LongContainersTest {
  @Test
  public void stressTestIndexConatiner() {
    //Arrange
    final int iterationNum = 1000;
    final int pageIdBound = 1 << 22;
    final int versionBound = 1 << 10;
    final int positionBound = 1 << 20;
    final int rangeBound = 1 << 12;

    for (int i = 0; i < iterationNum; i++) {
      final int pageId = ThreadLocalRandom.current().nextInt(0, pageIdBound);
      final int version = ThreadLocalRandom.current().nextInt(0, versionBound);
      final int position = ThreadLocalRandom.current().nextInt(0, positionBound);
      final int range = ThreadLocalRandom.current().nextInt(0, rangeBound);

      //Act
      final IndexLongContainer longContainer = new IndexLongContainer(pageId, version, position, range);

      //Assert
      Assert.assertEquals(pageId, longContainer.pageId());
      Assert.assertEquals(version, longContainer.version());
      Assert.assertEquals(position, longContainer.position());
      Assert.assertEquals(range, longContainer.range());

      for (int j = 0; j < iterationNum; j++) {
        final int newPosition = ThreadLocalRandom.current().nextInt(0, positionBound);
        final int newRange = ThreadLocalRandom.current().nextInt(0, rangeBound);

        //Act
        longContainer.setPosition(newPosition);
        longContainer.setRange(newRange);

        //Assert
        Assert.assertEquals(newPosition, longContainer.position());
        Assert.assertEquals(newRange, longContainer.range());
      }
    }
  }

  @Test
  public void stressTestPageConatiner() {
    //Arrange
    final int iterationNum = 100000;
    final int pageIdBound = 1 << 22;
    final int versionBound = 1 << 20;
    final int positionBound = 1 << 22;

    for (int i = 0; i < iterationNum; i++) {
      final int pageId = ThreadLocalRandom.current().nextInt(0, pageIdBound);
      final int version = ThreadLocalRandom.current().nextInt(0, versionBound);
      final int position = ThreadLocalRandom.current().nextInt(0, positionBound);

      //Act
      final PageLongContainer longContainer = new PageLongContainer(pageId, version, position);

      //Assert
      Assert.assertEquals(pageId, longContainer.pageId());
      Assert.assertEquals(version, longContainer.version());
      Assert.assertEquals(position, longContainer.position());
    }
  }
}
