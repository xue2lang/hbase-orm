package com.flipkart.hbaseobjectmapper.testcases.daos;


import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.testcases.entities.CitizenSummary;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class CitizenSummaryDAO extends AbstractHBDAO<String, CitizenSummary> {

    public CitizenSummaryDAO(Connection connection) throws IOException {
        super(connection);
    }
}
