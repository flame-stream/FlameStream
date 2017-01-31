package experiments.interfaces.artem.mockstream;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataItem {
  Meta meta();
  CharSequence serializedData();
  <T> T data(Class<T> type); //Add class to cast

  interface Meta {
    long time(); //It is more convenient to work with integers...
  }

  interface Grouping {
    long hash(DataItem item);
    boolean equals(DataItem left, DataItem right);
  }
}
