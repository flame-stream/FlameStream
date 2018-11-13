package com.spbsu.flamestream.example.bl.topwordcount;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordEntry;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;
import com.spbsu.flamestream.example.bl.topwordcount.ops.BucketedTopStatefulOp;
import com.spbsu.flamestream.example.bl.topwordcount.ops.CounterStatefulOp;
import com.spbsu.flamestream.example.bl.topwordcount.ops.Hashing;
import com.spbsu.flamestream.example.bl.topwordcount.ops.MapOp;
import com.spbsu.flamestream.example.bl.topwordcount.ops.SimplePipelineBuilder;
import com.spbsu.flamestream.example.bl.topwordcount.ops.TopTopStatefulOp;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TopWordCountGraph implements Supplier<Graph> {
  @Override
  public Graph get() {
    final Pattern pattern = Pattern.compile("\\s");
    SimplePipelineBuilder simplePipelineBuilder = new SimplePipelineBuilder();
    final SimplePipelineBuilder.Node counter = simplePipelineBuilder.node(
            new CounterStatefulOp(),
            new Hashing<WordContainer>() {
              @Override
              public int hash(WordContainer wordContainer) {
                return wordContainer.hashCode();
              }

              @Override
              public boolean equals(WordContainer left, WordContainer right) {
                return left.word().equals(right.word());
              }
            }
    );
    final SimplePipelineBuilder.Node splitter = simplePipelineBuilder.node(new MapOp<String, WordEntry>() {
      @Override
      public Class<String> inputClass() {
        return String.class;
      }

      @Override
      public Stream<WordEntry> apply(String s) {
        return Arrays.stream(pattern.split(s)).map(WordEntry::new);
      }
    });
    final SimplePipelineBuilder.Node bucketedTop = simplePipelineBuilder.node(
            new BucketedTopStatefulOp(2),
            new Hashing<Object>() {
              @Override
              public HashFunction hashFunction(HashFunction hashFunction) {
                return HashFunction.bucketedHash(hashFunction, 2);
              }
            }
    );
    final SimplePipelineBuilder.Node topTop = simplePipelineBuilder.node(
            new TopTopStatefulOp(2),
            new Hashing<WordsTop>() {}
    );
    simplePipelineBuilder.connectToSource(splitter);
    simplePipelineBuilder.connect(splitter, counter);
    simplePipelineBuilder.connect(counter, bucketedTop);
    simplePipelineBuilder.connect(bucketedTop, topTop);
    simplePipelineBuilder.connectToSink(topTop);
    return simplePipelineBuilder.build();
  }
}
