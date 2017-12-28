package com.spbsu.flamestream.example.bl.index.ranking;

/**
 * User: Artem
 * Date: 24.07.2017
 */
public class Document {
  private final int id;
  private final int version;

  public Document(int id, int version) {
    this.id = id;
    this.version = version;
  }

  public int id() {
    return id;
  }

  public int version() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Document document = (Document) o;
    return id == document.id && version == document.version;
  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + version;
    return result;
  }
}
