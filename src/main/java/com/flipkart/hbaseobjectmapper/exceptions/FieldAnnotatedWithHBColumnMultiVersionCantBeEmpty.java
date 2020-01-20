package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.annotations.HBColumnMultiVersion;

public class FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty extends IllegalArgumentException {
    public FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty() {
        super("Fields annotated with @" + HBColumnMultiVersion.class.getName() + " cannot be empty (null is ok, though)");
    }
}
