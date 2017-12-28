package com.spbsu.flamestream.example.bl.index.ranking;

import com.expleague.commons.math.MathTools;
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

  @SuppressWarnings("WeakerAccess")
  public Document document() {
    return document;
  }

  @SuppressWarnings("WeakerAccess")
  public double score() {
    return score;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Rank rank = (Rank) o;
    return Math.abs(rank.score() - score) < MathTools.EPSILON && document.equals(rank.document);
  }

  @Override
  public int hashCode() {
    int result = document.hashCode();
    final long temp = Double.doubleToLongBits(score);
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
