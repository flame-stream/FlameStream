package com.spbsu.flamestream.runtime.master.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.Joba;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.testng.Assert.*;

public class MinTimeUpdaterTest {

  @Test
  public void testOnShardMinTimeUpdate() {
    final ArrayList<Integer> shards = new ArrayList<>();
    shards.add(1);
    shards.add(2);
    final MinTimeUpdater<Integer> minTimeUpdater = new MinTimeUpdater<>(shards);
    final Joba.Id id = new Joba.Id("", "");
    final EdgeId edgeId = new EdgeId("", "");
    assertNull(minTimeUpdater.onShardMinTimeUpdate(
            2,
            new MinTimeUpdate(new GlobalTime(2, edgeId), new JobaTimes().updated(id, 1))
    ));
    final MinTimeUpdate minTimeUpdate = minTimeUpdater.onShardMinTimeUpdate(
            1,
            new MinTimeUpdate(new GlobalTime(1, edgeId), new JobaTimes().updated(id, 3))
    );
    assertNotNull(minTimeUpdate);
    assertEquals(minTimeUpdate.minTime(), new GlobalTime(1, edgeId));
  }
}
