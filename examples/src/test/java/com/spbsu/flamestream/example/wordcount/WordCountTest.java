package com.spbsu.flamestream.example.wordcount;

import com.spbsu.flamestream.example.AbstractExampleTest;
import com.spbsu.flamestream.example.FlameStreamExample;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public class WordCountTest extends AbstractExampleTest {

  @DataProvider
  public Object[][] testWordCountProvider() {
    return new Object[][]{
            {1}, {4}
    };
  }

  @Test(dataProvider = "testWordCountProvider")
  public void testWordCount(int workers) {
    test(WordCountCheckers.CHECK_COUNT, workers, 20);
  }

  @Override
  protected FlameStreamExample example() {
    return FlameStreamExample.WORD_COUNT;
  }
}
