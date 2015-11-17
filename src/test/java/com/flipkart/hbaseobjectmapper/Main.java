package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.daos.CitizenDAO;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import com.flipkart.hbaseobjectmapper.entities.Dependents;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;

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
        citizenDao.persist(new Citizen("IND", 101, "Manu", (short) 30, 30000, false, 2.3f, 4.33, 34L, new BigDecimal(100), 560034, new TreeMap<Long, Integer>() {
            {
                put(System.currentTimeMillis(), 100001);
            }
        }, new HashMap<String, Integer>() {
            {
                put("a", 1);
                put("b", 1);
            }
        }, new Dependents(121, Arrays.asList(122, 123))));
        citizenDao.delete("IND#101");
    }
}
