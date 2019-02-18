package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;

import java.util.function.Function;
import java.util.stream.Stream;

public class IDFObjectCompleteFilter implements Function<IdfObject, Stream<IdfObject>> {

  @Override
  public Stream<IdfObject> apply(IdfObject idfObject) {
    if (idfObject.isComplete()) {
      return Stream.of(idfObject);
    } else {
      return Stream.empty();
    }
  }
}
