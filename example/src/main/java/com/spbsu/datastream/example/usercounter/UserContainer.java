package com.spbsu.datastream.example.usercounter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
    @JsonSubTypes.Type(value=UserCounter.class, name="stat"),
    @JsonSubTypes.Type(value=UserQuery.class, name="log")
})
public interface UserContainer {
  String user();
}
