package experiments.interfaces.vova.impl;

import experiments.interfaces.vova.impl.mock.CharItem;
import experiments.interfaces.vova.impl.mock.Filters.CountFilter;
import experiments.interfaces.vova.impl.mock.IntType;
import experiments.interfaces.vova.impl.mock.Filters.SqrFilter;
import experiments.interfaces.vova.impl.mock.MetaImp;

import java.util.ArrayList;

class Main {
    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        testCounterAndCommit();
    }

    public static void testCounterAndCommit(){
        ArrayList<CharItem> a = new ArrayList<>();
        for(int i = 0; i < 100; i++){
            boolean commit = i%30 == 0;
            boolean fail = i%40 == 0;
            a.add(new CharItem("123", new MetaImp(i, commit, fail)));
        }
        FilterIterator<CharItem> f = new FilterIterator<>(a.iterator(), new CountFilter(), new IntType());
        for(int i =0; i < 100; i++){
            System.out.println(f.next().serializedData());
        }
    }

    public static void testMerger(){
        ArrayList<CharItem> a = new ArrayList<>();
        a.add(new CharItem("a", new MetaImp(0, false, false)));
        a.add(new CharItem("2", new MetaImp(0, false, false)));
        ArrayList<CharItem> b = new ArrayList<>();
        b.add(new CharItem("b", new MetaImp(0, false, false)));
        b.add(new CharItem("4", new MetaImp(0, false, false)));
        MergingIterator<CharItem> m = new MergingIterator<CharItem>(a.iterator(), b.iterator());
        FilterIterator<CharItem> f = new FilterIterator<>(m, new SqrFilter(), new IntType());
        System.out.println(f.next().serializedData());
        System.out.println(f.next().serializedData());
        System.out.println(f.next().serializedData());
        System.out.println(f.next().serializedData());
    }

    public static void testSqrFilter(){
        ArrayList<CharItem> strs = new ArrayList<>();
        strs.add(new CharItem("a", new MetaImp(0, false, false)));
        strs.add(new CharItem("2", new MetaImp(0, false, false)));
        FilterIterator<CharItem> f = new FilterIterator<>(strs.iterator(), new SqrFilter(), new IntType());
        FilterIterator<CharItem> g = new FilterIterator<>(f, new SqrFilter(), new IntType());
        System.out.println(g.next().serializedData().toString());
        System.out.println(g.next().serializedData().toString());
    }
}
