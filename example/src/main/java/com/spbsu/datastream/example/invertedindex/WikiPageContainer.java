package com.spbsu.datastream.example.invertedindex;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Author: Artem
 * Date: 18.01.2017
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = WikiPagePositionState.class, name = "state"),
        @JsonSubTypes.Type(value = WordOutput.class, name = "output")
})
public interface WikiPageContainer {
  int pageId();
}
