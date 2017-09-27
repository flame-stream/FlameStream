package com.spbsu.flamestream.example.inverted_index;

import com.spbsu.commons.text.stem.Stemmer;
import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordContainer;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexRemove;
import org.testng.Assert;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public enum InvertedIndexCheckers implements ExampleChecker<WikipediaPage, WordContainer> {
  CHECK_INDEX_WITH_SMALL_DUMP {
    @Override
    public Stream<WikipediaPage> input() {
      return WikipeadiaInput.dumpStreamFromResources("wikipedia/test_index_small_dump.xml");
    }

    @Override
    public void check(Stream<WordContainer> output) {
      Assert.assertEquals(output.count(), 3481);
      { //assertions for word "isbn"
        final String isbn = stem("isbn");
        Assert.assertTrue(output
                .filter(wordContainer -> isbn.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                        {
                                IndexItemInLong.createPagePosition(7, 2534, 1),
                                IndexItemInLong.createPagePosition(7, 2561, 1)
                        })));
        Assert.assertTrue(output
                .filter(wordContainer -> isbn.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
                .allMatch(indexRemove ->
                        ((WordIndexRemove) indexRemove).start() == IndexItemInLong.createPagePosition(7, 2534, 1) && ((WordIndexRemove) indexRemove).range() == 2));
        Assert.assertTrue(output
                .filter(wordContainer -> isbn.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                        {
                                IndexItemInLong.createPagePosition(7, 2561, 2)
                        })));
      }
      { //assertions for word "вставка"
        final String vstavka = stem("вставка");
        Assert.assertTrue(output
                .filter(wordContainer -> vstavka.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .allMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                        {
                                IndexItemInLong.createPagePosition(7, 2515, 2)
                        })));
        Assert.assertTrue(output
                .filter(wordContainer -> vstavka.equals(wordContainer.word()))
                .noneMatch(wordContainer -> wordContainer instanceof WordIndexRemove));
      }
      { //assertions for word "эйдинтас"
        final String eidintas = stem("эйдинтас");
        Assert.assertTrue(output
                .filter(wordContainer -> eidintas.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                        {
                                IndexItemInLong.createPagePosition(7, 2516, 1)
                        })));
        Assert.assertTrue(output
                .filter(wordContainer -> eidintas.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
                .allMatch(indexRemove ->
                        ((WordIndexRemove) indexRemove).start() == IndexItemInLong.createPagePosition(7, 2516, 1) && ((WordIndexRemove) indexRemove).range() == 1));
        Assert.assertTrue(output
                .filter(wordContainer -> eidintas.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                        {
                                IndexItemInLong.createPagePosition(7, 2517, 2)
                        })));
      }
    }
  };

  private static String stem(String term) {
    //noinspection deprecation
    final Stemmer stemmer = Stemmer.getInstance();
    return stemmer.stem(term).toString();
  }
}
