package com.spbsu.datastream.core.inference;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.spbsu.datastream.core.DataType;

import java.util.*;

/**
 * Created by marnikitta on 05.11.16.
 */
public class TypeGraph {
  private Set<DataType> nodes = new HashSet<>();

  private Multimap<DataType, Morphism> outgoing = HashMultimap.create();

  public void addMorphism(Morphism morphism) {
    nodes.add(morphism.consumes());
    nodes.add(morphism.supplies());
    outgoing.put(morphism.consumes(), morphism);
  }

  public List<Morphism> findPath(final DataType from, final DataType to) {
    if (!nodes.contains(from) || !nodes.contains(to)) {
      throw new NoSuchElementException();
    }

    final Map<DataType, Morphism> parentNode = new HashMap<>();
    final Set<DataType> visited = new HashSet<>();
    final Queue<DataType> bfs = new ArrayDeque<>();

    bfs.offer(from);

    while (!bfs.isEmpty()) {
      final DataType current = bfs.poll();
      visited.add(current);

      if (current.equals(to)) {
        break;
      }

      for (Morphism e : outgoing.get(current)) {
        if (!visited.contains(e.supplies())) {
          bfs.offer(e.supplies());
          parentNode.put(e.supplies(), e);
        }
      }
    }

    if (parentNode.containsKey(to)) {
      final Deque<Morphism> stack = new ArrayDeque<>();
      DataType currentType = to;

      while (!currentType.equals(from)) {
        final Morphism tmp = parentNode.get(currentType);
        stack.push(tmp);
        currentType = tmp.consumes();
      }

      final List<Morphism> result = new ArrayList<>();
      while (!stack.isEmpty()) {
        result.add(stack.pop());
      }
      return result;
    } else {
      return Collections.emptyList();
    }
  }
}
