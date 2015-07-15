package com.flipkart.hbaseobjectmapper.exceptions;

public class MappedColumnCantBeTransientException extends IllegalArgumentException {
    public MappedColumnCantBeTransientException(String s) {
        super(s);
    }
}
