package com.flipkart.hbaseobjectmapper.entities;


import com.flipkart.hbaseobjectmapper.*;

import java.util.NavigableMap;

public class TestClassesHBColumnMultiVersion {
    @HBTable("test_history")
    public static class Versionless implements HBRecord {
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

        @HBColumn(family = "f", column = "c")
        private Double c;

        public Versionless() {

        }

        public Versionless(Double c) {
            this.c = c;
        }
    }

    @HBTable("test_history")
    public static class Versioned implements HBRecord {
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
        private NavigableMap<Long, Double> c;

        public Versioned() {

        }

        public NavigableMap<Long, Double> getC() {
            return c;
        }
    }
}