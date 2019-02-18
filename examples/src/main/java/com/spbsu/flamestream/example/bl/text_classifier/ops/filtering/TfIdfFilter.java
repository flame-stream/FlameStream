package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.IDFObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class TfIdfFilter implements Function<List<DocContainer>, Stream<DocContainer>> {

  @Override
  public Stream<DocContainer> apply(List<DocContainer> elements) {
    if (elements.size() < 2 || !elements.get(0).document().equals(elements.get(1).document())) {
      return Stream.of();
    } else {
      if (elements.get(0) instanceof TfIdfObject && elements.get(1) instanceof  IDFObject) {
        elements = Arrays.asList(elements.get(1), elements.get(0));
      }
      IDFObject idf = (IDFObject)elements.get(0);
      TfIdfObject tf = (TfIdfObject)elements.get(1);
      TfIdfObject res = new TfIdfObject(tf, idf.counts());
      return Stream.of(res);
    }
  }
}
