package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.codec.Codec;

/**
 * A flag for {@link Codec Codec} (specify parameter name and value)
 * <p>
 * This is to be used exclusively for input to {@link HBColumn#codecFlags() codecFlags} parameter of {@link HBColumn} and {@link HBColumnMultiVersion} annotations
 */
public @interface Flag {
    String name();

    String value();
}
