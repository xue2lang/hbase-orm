package com.flipkart.hbaseobjectmapper.exceptions;

import com.flipkart.hbaseobjectmapper.annotations.HBColumn;
import com.flipkart.hbaseobjectmapper.annotations.HBColumnMultiVersion;

public class FieldNotMappedToHBaseColumnException extends IllegalArgumentException {
    public FieldNotMappedToHBaseColumnException(Class<?> hbRecordClass, String fieldName) {
        super(String.format("Field %s.%s is not mapped to an HBase column (consider adding %s or %s annotation)", hbRecordClass.getSimpleName(), fieldName, HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()));
    }
}
