package experiments.interfaces.solar;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataItem {
  Meta meta();
  CharSequence serializedData();
  <T> T data();

  interface Meta {
    double time();
  }

  interface Grouping {
    long hash(DataItem item);
    boolean equals(DataItem left, DataItem right);
  }
}
