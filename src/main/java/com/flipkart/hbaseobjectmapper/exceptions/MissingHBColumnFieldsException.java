package com.flipkart.hbaseobjectmapper.exceptions;

public class MissingHBColumnFieldsException extends IllegalArgumentException {
    public MissingHBColumnFieldsException(String s) {
        super(s);
    }
}
