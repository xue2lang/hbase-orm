package com.flipkart.hbaseobjectmapper.exceptions;

public class FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty extends IllegalArgumentException {
    public FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty(String s) {
        super(s);
    }
}
