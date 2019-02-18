package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.IDFObject;

import java.util.function.Function;
import java.util.stream.Stream;

public class IDFObjectCompleteFilter implements Function<IDFObject, Stream<IDFObject>> {

  @Override
  public Stream<IDFObject> apply(IDFObject idfObject) {
    if (idfObject.isComplete()) {
      return Stream.of(idfObject);
    } else {
      return Stream.empty();
    }
  }
}
