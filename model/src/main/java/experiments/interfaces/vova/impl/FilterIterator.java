package experiments.interfaces.vova.impl;


import experiments.interfaces.vova.Condition;
import experiments.interfaces.vova.DataItem;
import experiments.interfaces.vova.Filter;
import experiments.interfaces.vova.Type;
import scala.concurrent.java8.FuturesConvertersImpl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class FilterIterator<T extends DataItem> implements Iterator<T> {

    public FilterIterator(Iterator<T> source, Filter filter, Type inputType){
        conditions = new HashSet<>();
        verified = new HashSet<>();
        for(Class<? extends Condition> c: inputType.conditions()){
            try {
                conditions.add(c.newInstance());
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        this.source = source;
        this.filter = filter;
    }

    public Set<Class<? extends Condition>> getVerified(){
        return verified;
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        T next = source.next();
        CharSequence data = next.serializedData();
        if(check(next)){
            next.setSerializedData(filter.apply(data));
        }else{
            next.meta().makeBad();
        }

        if(next.meta().commit()){
            filter.commit();
        }
        if(next.meta().fail()){
            filter.fail();
        }

        return next;
    }

    private boolean check(DataItem item){
        boolean error = false;
        for (Condition c: conditions){
            if(!c.update(item)){  //or c.update(item)?
                verified.add(c.getClass());
                error = true;
            }
        }
        return !error;
    }

    private Set<Condition> conditions;
    private Set<Class<? extends Condition>> verified;
    private Iterator<T> source;
    private Filter filter;
}
