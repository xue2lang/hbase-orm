package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.annotations.HBColumn;
import com.flipkart.hbaseobjectmapper.annotations.HBColumnMultiVersion;

public class MissingHBColumnFieldsException extends IllegalArgumentException {
    public MissingHBColumnFieldsException(Class<?> clazz) {
        super(String.format("Class %s doesn't even have a single field annotated with @%s or @%s", clazz.getName(), HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()));
    }
}
