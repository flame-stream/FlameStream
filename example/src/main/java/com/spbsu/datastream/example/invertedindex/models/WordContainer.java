package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Author: Artem
 * Date: 18.01.2017
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = WordOutput.class, name = "diff"),
        @JsonSubTypes.Type(value = WordIndex.class, name = "state")
})
public interface WordContainer {
  String word();
}
