package com.flipkart.hbaseobjectmapper.exceptions;

public class RowKeyCantBeEmptyException extends IllegalArgumentException {
    public RowKeyCantBeEmptyException(String s) {
        super(s);
    }
}
