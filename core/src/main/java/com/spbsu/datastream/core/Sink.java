package com.spbsu.datastream.core;

import com.spbsu.datastream.core.job.control.Control;

public interface Sink {
    void accept(DataItem item);
    void accept(Control control);
}