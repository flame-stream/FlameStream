package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Created by marnikitta on 28.11.16.
 */
public class TypeGraphTest {

  @Test
  public void testShortFindPath() throws Exception {
    final DataType t1 = new DataType.Stub("t1");
    final DataType t2 = new DataType.Stub("t2");
    final Morphism m = new StubMorphism(t1, t2);

    final TypeGraph graph = new TypeGraph();
    graph.addMorphism(m);

    final List<Morphism> result = graph.findPath(t1, t2);

    Assert.assertEquals(result, Collections.singletonList(m));
  }

  @Test
  public void testLongFindPath() throws Exception {
    final DataType t1 = new DataType.Stub("t1");
    final DataType t2 = new DataType.Stub("t2");
    final DataType t3 = new DataType.Stub("t3");
    final DataType t4 = new DataType.Stub("t4");

    final Morphism m1 = new StubMorphism(t1, t2);
    final Morphism m2 = new StubMorphism(t2, t3);
    final Morphism m3 = new StubMorphism(t3, t4);

    final TypeGraph graph = new TypeGraph();
    graph.addMorphism(m1);
    graph.addMorphism(m2);
    graph.addMorphism(m3);

    final List<Morphism> result = graph.findPath(t1, t4);

    final List<Morphism> expected = Arrays.asList(m1, m2, m3);
    Assert.assertEquals(result, expected);
  }

  @Test
  public void testCycleFindPath() throws Exception {
    final DataType t1 = new DataType.Stub("t1");
    final DataType t2 = new DataType.Stub("t2");
    final DataType t3 = new DataType.Stub("t3");
    final DataType t4 = new DataType.Stub("t4");

    final Morphism m1 = new StubMorphism(t1, t2);
    final Morphism m2 = new StubMorphism(t2, t1);
    final Morphism m3 = new StubMorphism(t2, t3);

    final TypeGraph graph = new TypeGraph();
    graph.addMorphism(m1);
    graph.addMorphism(m2);
    graph.addMorphism(m3);

    final List<Morphism> result = graph.findPath(t1, t3);

    final List<Morphism> expected = Arrays.asList(m1, m3);
    Assert.assertEquals(result, expected);
  }
}