package com.spbsu.datastream.example.bl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spbsu.datastream.example.bl.counter.UserCounter;
//import com.spbsu.datastream.example.bl.sql.UserSelector;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
    @JsonSubTypes.Type(value=UserCounter.class, name="stat"),
//    @JsonSubTypes.Type(value= UserSelector.class, name="selector"),
    @JsonSubTypes.Type(value=UserQuery.class, name="log")
})
public interface UserContainer {
  String user();
}
