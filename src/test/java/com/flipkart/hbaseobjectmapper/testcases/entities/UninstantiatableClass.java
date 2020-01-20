package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;
import com.flipkart.hbaseobjectmapper.annotations.Family;
import com.flipkart.hbaseobjectmapper.annotations.HBColumn;
import com.flipkart.hbaseobjectmapper.annotations.HBRowKey;
import com.flipkart.hbaseobjectmapper.annotations.HBTable;

@SuppressWarnings("unused")
@HBTable(name = "blah", families = {@Family(name = "main")})
public class UninstantiatableClass implements HBRecord<String> {
    @HBRowKey
    private Integer uid;
    @HBColumn(family = "main", column = "name")
    private String name;

    public UninstantiatableClass() {
        throw new RuntimeException("I'm a bad constructor");
    }

    @Override
    public String composeRowKey() {
        return uid.toString();
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.uid = Integer.valueOf(rowKey);
    }
}
