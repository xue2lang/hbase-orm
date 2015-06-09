package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.daos.CitizenDAO;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Created by manu.manjunath on 08/06/15.
 */
public class Main {
    public static void main(String args[]) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.setInt("hbase.client.scanner.caching", 1000);
        System.out.println("config");
        CitizenDAO citizenDao = new CitizenDAO(conf);
        System.out.println("dao");
        for (Citizen e : TestObjects.validObjs) {
            final String rowKey = citizenDao.persist(e);
            System.out.println(rowKey);
        }

    }
}
