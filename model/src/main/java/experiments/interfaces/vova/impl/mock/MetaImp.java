package experiments.interfaces.vova.impl.mock;

import experiments.interfaces.vova.DataItem;

/**
 * Created by nuka3 on 11/6/16.
 */
public class MetaImp implements CharItem.Meta{
    public MetaImp(long time, boolean commit, boolean fail){
        this.time = time;
        this.commit = commit;
        this.fail = fail;
        good = true;
    }

    @Override
    public long time() {
        return time;
    }

    @Override
    public boolean commit() {
        return commit;
    }

    @Override
    public boolean fail() {
        return fail;
    }

    @Override
    public boolean good() {
        return good;
    }

    @Override
    public void makeBad() {
        good = false;
    }


    long time;
    boolean good;
    boolean commit;
    boolean fail;
}
