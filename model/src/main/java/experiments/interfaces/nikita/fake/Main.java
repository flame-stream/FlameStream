package experiments.interfaces.nikita.fake;

import experiments.interfaces.nikita.YetAnotherStream;
import scala.Int;

import java.util.Random;

/**
 * Created by marnikitta on 19.10.16.
 */
public class Main {
    public static void main(String[] args) {
        Random rd = new Random();
        YetAnotherStream<Integer> stream = new InMemoryStream<>(() -> rd.nextInt() * 2, new OddNumber(101), 100);
        System.out.println(stream.isValid());
    }
}
