package experiments.interfaces.artem.mockstream.impl;

import experiments.interfaces.artem.mockstream.Condition;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

class Main {
    public static void main(String[] args) {
        Set<Condition> violated = new HashSet<>();
        violated.add(new NotZeroCondition());

        Random random = new Random();
        MockDataStream mockDataStream = MockDataStream.generate(() -> new IntDataItem(random.nextInt()), violated);

        System.out.print(mockDataStream.isValid());
    }
}