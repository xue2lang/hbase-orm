package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.WrappedHBColumn;

import java.lang.reflect.Field;

public class MappedColumnCantBeStaticException extends IllegalArgumentException {
    public MappedColumnCantBeStaticException(Field field, WrappedHBColumn hbColumn) {
        super(String.format("In class \"%s\", the field \"%s\" is annotated with \"%s\", but is declared as static (Only instance fields can be mapped to HBase columns)", field.getDeclaringClass().getName(), field.getName(), hbColumn.getName()));
    }
}
