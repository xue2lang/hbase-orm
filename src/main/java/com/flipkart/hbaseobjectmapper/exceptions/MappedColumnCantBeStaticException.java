package com.flipkart.hbaseobjectmapper.exceptions;

public class MappedColumnCantBeStaticException extends IllegalArgumentException {
    public MappedColumnCantBeStaticException(String s) {
        super(s);
    }
}
