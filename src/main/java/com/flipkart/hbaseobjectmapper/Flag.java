package com.flipkart.hbaseobjectmapper;

/**
 * Flag for {@link com.flipkart.hbaseobjectmapper.codec.Codec Codec}. Optionally, to be passed as input to {@link HBColumn#codecFlags() codecFlags} parameter of {@link HBColumn} and {@link HBColumnMultiVersion} annotations.
 */
public @interface Flag {
    String name();

    String value();
}
