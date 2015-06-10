package com.flipkart.hbaseobjectmapper.daos;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.entities.TestClassesHBColumnMultiVersion.Versionless;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class VersionlessDAO extends AbstractHBDAO<Versionless> {

    public VersionlessDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
