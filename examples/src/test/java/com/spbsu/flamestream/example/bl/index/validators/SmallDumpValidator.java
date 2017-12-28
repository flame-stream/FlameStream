package com.spbsu.flamestream.example.bl.index.validators;

import com.spbsu.flamestream.example.bl.index.InvertedIndexValidator;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class SmallDumpValidator extends InvertedIndexValidator.Stub {
  @Override
  public Stream<WikipediaPage> input() {
    return WikipeadiaInput.dumpStreamFromResources("wikipedia/test_index_small_dump.xml");
  }

  @Override
  public void assertCorrect(Stream<WordBase> output) {
    final List<WordBase> outputList = new ArrayList<>();
    output.forEach(outputList::add);
    Assert.assertEquals(outputList.size(), 3481);
    { //assertions for word "isbn"
      final String isbn = stem("isbn");
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> isbn.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                      IndexItemInLong.createPagePosition(7, 2534, 1), IndexItemInLong.createPagePosition(7, 2561, 1)
              })));
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> isbn.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
              .allMatch(indexRemove ->
                      ((WordIndexRemove) indexRemove).start() == IndexItemInLong.createPagePosition(7, 2534, 1)
                              && ((WordIndexRemove) indexRemove).range() == 2));
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> isbn.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                      IndexItemInLong.createPagePosition(7, 2561, 2)
              })));
    }
    { //assertions for word "вставка"
      final String vstavka = stem("вставка");
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> vstavka.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .allMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                      IndexItemInLong.createPagePosition(7, 2515, 2)
              })));
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> vstavka.equals(wordContainer.word()))
              .noneMatch(wordContainer -> wordContainer instanceof WordIndexRemove));
    }
    { //assertions for word "эйдинтас"
      final String eidintas = stem("эйдинтас");
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> eidintas.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                      IndexItemInLong.createPagePosition(7, 2516, 1)
              })));
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> eidintas.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
              .allMatch(indexRemove ->
                      ((WordIndexRemove) indexRemove).start() == IndexItemInLong.createPagePosition(7, 2516, 1)
                              && ((WordIndexRemove) indexRemove).range() == 1));
      Assert.assertTrue(outputList.stream()
              .filter(wordContainer -> eidintas.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                      IndexItemInLong.createPagePosition(7, 2517, 2)
              })));
    }
  }

  @Override
  public int expectedOutputSize() {
    return 3481; //experimentally computed
  }
}
