package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.flipkart.hbaseobjectmapper.*;

import com.flipkart.hbaseobjectmapper.annotations.Family;
import com.flipkart.hbaseobjectmapper.annotations.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.annotations.HBRowKey;
import com.flipkart.hbaseobjectmapper.annotations.HBTable;
import java.util.Map;
import java.util.NavigableMap;

public class ClassesWithFieldIncompatibleWithHBColumnMultiVersion {
    @SuppressWarnings("unused")
    @HBTable(name = "blah", families = {@Family(name = "f")})
    public static class NotMap implements HBRecord<String> {
        @HBRowKey
        protected String key = "key";

        @Override
        public String composeRowKey() {
            return key;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.key = rowKey;
        }

        @HBColumnMultiVersion(family = "f", column = "c")
        private Integer i;
    }

    @SuppressWarnings("unused")
    @HBTable(name = "blah", families = {@Family(name = "f")})
    public static class NotNavigableMap implements HBRecord<String> {
        @HBRowKey
        protected String key = "key";

        @Override
        public String composeRowKey() {
            return key;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.key = rowKey;
        }

        @HBColumnMultiVersion(family = "f", column = "c")
        private Map<Long, Integer> i;
    }

    @SuppressWarnings("unused")
    @HBTable(name = "blah", families = {@Family(name = "f")})
    public static class EntryKeyNotLong implements HBRecord<String> {
        @HBRowKey
        protected String key = "key";

        @Override
        public String composeRowKey() {
            return key;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.key = rowKey;
        }

        @HBColumnMultiVersion(family = "f", column = "c")
        private NavigableMap<Integer, Integer> i;
    }
}
