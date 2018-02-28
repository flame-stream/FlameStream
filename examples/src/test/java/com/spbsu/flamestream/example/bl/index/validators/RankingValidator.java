package com.spbsu.flamestream.example.bl.index.validators;

import com.spbsu.flamestream.example.bl.index.InvertedIndexValidator;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.ranking.Document;
import com.spbsu.flamestream.example.bl.index.ranking.Rank;
import com.spbsu.flamestream.example.bl.index.ranking.RankingFunction;
import com.spbsu.flamestream.example.bl.index.ranking.RankingStorage;
import com.spbsu.flamestream.example.bl.index.ranking.impl.BM25;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import org.testng.Assert;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class RankingValidator extends InvertedIndexValidator.Stub {
  @Override
  public Stream<WikipediaPage> input() {
    return WikipeadiaInput.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml");
  }

  @Override
  public void assertCorrect(Stream<WordBase> output) {
    final RankingStorage rankingStorage = rankingStorage(output);
    final RankingFunction rankingFunction = new BM25(rankingStorage);
    {
      final Stream<Rank> result = rankingFunction.rank("Бразилия Пеле");
      final Rank[] topResults = result.sorted().limit(5).toArray(Rank[]::new);
      Assert.assertEquals(topResults[0], new Rank(new Document(51626, 1), 0.01503891930975921));
      Assert.assertEquals(topResults[1], new Rank(new Document(1027839, 1), 0.013517410031763473));
      Assert.assertEquals(topResults[2], new Rank(new Document(2446853, 1), 0.010903350643125045));
      Assert.assertEquals(topResults[3], new Rank(new Document(227209, 1), 0.008340914850280897));
      Assert.assertEquals(topResults[4], new Rank(new Document(229964, 1), 0.00632081101215173));
    }
    {
      final Stream<Rank> result = rankingFunction.rank("Аргентина Марадона");
      final Rank[] topResults = result.sorted().limit(5).toArray(Rank[]::new);
      Assert.assertEquals(topResults[0], new Rank(new Document(227209, 1), 0.03466819792138674));
      Assert.assertEquals(topResults[1], new Rank(new Document(688695, 1), 0.034573538000985574));
      Assert.assertEquals(topResults[2], new Rank(new Document(879050, 1), 0.030395004860259645));
      Assert.assertEquals(topResults[3], new Rank(new Document(2446853, 1), 0.026082172662643795));
      Assert.assertEquals(topResults[4], new Rank(new Document(1020395, 1), 0.0133369643808426));
    }
  }

  @Override
  public int expectedOutputSize() {
    return 65813; //experimentally computed
  }
}
