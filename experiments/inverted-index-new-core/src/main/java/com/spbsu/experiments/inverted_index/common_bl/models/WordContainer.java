package com.spbsu.experiments.inverted_index.common_bl.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spbsu.experiments.inverted_index.datastreams.models.WordAddOutput;
import com.spbsu.experiments.inverted_index.datastreams.models.WordRemoveOutput;

/**
 * Author: Artem
 * Date: 18.01.2017
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = WordAddOutput.class, name = "add"),
        @JsonSubTypes.Type(value = WordRemoveOutput.class, name = "remove"),
        @JsonSubTypes.Type(value = WordIndex.class, name = "state")
})
public interface WordContainer {
  String word();
}
