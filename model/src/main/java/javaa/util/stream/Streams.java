/*
 * Copyright (c) 2012, 2013, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
package javaa.util.stream;

/**
 * Utility methods for operating on and creating streams.
 * <p>
 * <p>Unless otherwise stated, streams are created as sequential streams.  A
 * sequential stream can be transformed into a parallel stream by calling the
 * {@code parallel()} method on the created stream.
 *
 * @since 1.8
 */
final class Streams {

    static Runnable composeWithExceptions(Runnable a, Runnable b) {
        return () -> {
            try {
                a.run();
            } catch (Throwable e1) {
                try {
                    b.run();
                } catch (Throwable e2) {
                    try {
                        e1.addSuppressed(e2);
                    } catch (Throwable ignore) {
                    }
                }
                throw e1;
            }
            b.run();
        };
    }
}
