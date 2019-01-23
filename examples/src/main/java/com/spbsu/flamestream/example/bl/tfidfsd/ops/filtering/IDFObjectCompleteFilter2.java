package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.DocEntry;

import java.util.function.Function;
import java.util.stream.Stream;

public class IDFObjectCompleteFilter2 implements Function<IDFObject, Stream<DocContainer>> {

    @Override
    public Stream<DocContainer> apply(IDFObject idfObject) {
        if (idfObject.isComplete()) {
            //System.out.println("9999999999999: " + idfObject);
            return Stream.of(new DocEntry(idfObject.document(), 1));
        } else {
            return Stream.of(idfObject);
        }
    }
}
