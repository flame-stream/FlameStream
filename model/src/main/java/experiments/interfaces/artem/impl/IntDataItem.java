package experiments.interfaces.artem.impl;

import experiments.interfaces.artem.DataItem;

class IntDataItem implements DataItem {
    private Integer _data;
    private Meta _meta;

    IntDataItem(Integer data) {
        _data = data;
        _meta = new SystemTimeMeta();
    }

    @Override
    public Meta meta() {
        return _meta;
    }

    @Override
    public CharSequence serializedData() {
        return null;
    }

    @Override
    public <T> T data(Class<T> type) {
        return type.cast(_data);
    }
}