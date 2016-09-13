package com.flipkart.hbaseobjectmapper.daos;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.HBRecord;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class DAOContainer {
    private static class ClassWithShortRowKey implements HBRecord<Short> {

        private Short rowKey;

        public ClassWithShortRowKey(Short rowKey) {
            this.rowKey = rowKey;
        }

        @Override
        public Short composeRowKey() {
            return rowKey;
        }

        @Override
        public void parseRowKey(Short rowKey) {
            this.rowKey = rowKey;
        }
    }

    public static class ShortDAO extends AbstractHBDAO<Short, ClassWithShortRowKey> {

        protected ShortDAO(Configuration conf) throws IOException {
            super(conf);
        }
    }

}
