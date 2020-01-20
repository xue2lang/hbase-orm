package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.annotations.HBColumn;
import com.flipkart.hbaseobjectmapper.annotations.MappedSuperClass;

@MappedSuperClass
public class AbstractRecord {

    @HBColumn(family = "a", column = "created_at")
    protected Long createdAt;
}
