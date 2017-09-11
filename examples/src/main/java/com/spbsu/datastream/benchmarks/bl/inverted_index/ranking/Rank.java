package com.spbsu.datastream.benchmarks.bl.inverted_index.ranking;

import com.spbsu.commons.math.MathTools;
import org.jetbrains.annotations.NotNull;

/**
 * User: Artem
 * Date: 30.07.2017
 */
public class Rank implements Comparable<Rank> {
  private final Document document;
  private final double score;

  public Rank(Document document, double score) {
    this.document = document;
    this.score = score;
  }

  public Document document() {
    return document;
  }

  public double score() {
    return score;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Rank rank = (Rank) o;
    return Math.abs(rank.score() - score) < MathTools.EPSILON && document.equals(rank.document);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = document.hashCode();
    temp = Double.doubleToLongBits(score);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public int compareTo(@NotNull Rank o) {
    final int scoreCompare = Double.compare(o.score(), score);
    if (scoreCompare == 0) {
      return Integer.compare(document.id(), o.document().id());
    }
    return scoreCompare;
  }
}
