package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;

import java.util.NavigableMap;

public class HBColumnMultiVersionFieldInvalidDataTypeException extends IllegalArgumentException {
    public HBColumnMultiVersionFieldInvalidDataTypeException() {
        super(String.format("A field annotated with @%s should be of type %s<%s, ?>", HBColumnMultiVersion.class.getName(), NavigableMap.class.getName(), Long.class.getName()));
    }
}
