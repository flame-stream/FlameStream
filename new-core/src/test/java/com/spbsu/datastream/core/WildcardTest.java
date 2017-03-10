package com.spbsu.datastream.core;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class WildcardTest {
  @Test
  public void test() {
    List<String> strLi = new ArrayList<>();
    strLi.add("Abasaba");

    List<?> li = strLi;
    Object a = li.get(0);

    List<? extends String> lis = strLi;

    lis.add(null);
  }
}
