package experiments.interfaces.artem.iterators.test;

import experiments.interfaces.artem.iterators.CommitFailIterator;
import experiments.interfaces.artem.iterators.impl.ReplicatingIterator;
import experiments.interfaces.artem.mockstream.impl.IntDataItem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ReplicatingIteratorTest {
  private CommitFailIterator<IntDataItem> replicatingIterator;

  @Before
  public void beforeEach() {
    List<IntDataItem> dataItems = new ArrayList<>();
    final int listSize = 10;
    for (int i = 1; i <= listSize; i++) {
      dataItems.add(new IntDataItem(i));
    }
    replicatingIterator = new ReplicatingIterator<>(dataItems.iterator());
  }

  @Test
  public void failCommitTest() {
    Assert.assertEquals(new IntDataItem(1), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(2), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(3), replicatingIterator.next());

    replicatingIterator.fail();

    Assert.assertEquals(new IntDataItem(1), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(2), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(3), replicatingIterator.next());

    replicatingIterator.fail();

    Assert.assertEquals(new IntDataItem(1), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(2), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(3), replicatingIterator.next());

    replicatingIterator.commit();

    Assert.assertEquals(new IntDataItem(4), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(5), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(6), replicatingIterator.next());
  }

  @Test
  public void commitFailCommitTest() {
    Assert.assertEquals(new IntDataItem(1), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(2), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(3), replicatingIterator.next());

    replicatingIterator.commit();

    Assert.assertEquals(new IntDataItem(4), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(5), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(6), replicatingIterator.next());

    replicatingIterator.fail();

    Assert.assertEquals(new IntDataItem(4), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(5), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(6), replicatingIterator.next());

    replicatingIterator.fail();

    Assert.assertEquals(new IntDataItem(4), replicatingIterator.next());

    replicatingIterator.commit();

    Assert.assertEquals(new IntDataItem(5), replicatingIterator.next());
    Assert.assertEquals(new IntDataItem(6), replicatingIterator.next());
  }
}
