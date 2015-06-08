package com.flipkart.hbaseobjectmapper.exceptions;

public class BothHBColumnAnnotationsPresentException extends IllegalArgumentException {
    public BothHBColumnAnnotationsPresentException(String message) {
        super(message);
    }
}
