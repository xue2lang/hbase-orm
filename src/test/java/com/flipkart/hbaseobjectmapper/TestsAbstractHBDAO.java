package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.daos.CitizenDAO;
import com.flipkart.hbaseobjectmapper.daos.CitizenSummaryDAO;
import com.flipkart.hbaseobjectmapper.daos.VersionlessDAO;
import com.flipkart.hbaseobjectmapper.entities.Citizen;
import com.flipkart.hbaseobjectmapper.entities.TestClassesHBColumnMultiVersion.Versionless;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class TestsAbstractHBDAO {
    HBaseTestingUtility utility = new HBaseTestingUtility();
    Configuration configuration;
    CitizenDAO citizenDao;
    CitizenSummaryDAO citizenSummaryDAO;
    VersionlessDAO versionlessDAO;
    List<Citizen> testObjs = TestObjects.validObjs;
    final static long CLUSTER_START_TIMEOUT = 30;

    class ClusterStarter implements Callable<MiniHBaseCluster> {
        private final HBaseTestingUtility utility;

        public ClusterStarter(HBaseTestingUtility utility) {
            this.utility = utility;
        }

        @Override
        public MiniHBaseCluster call() throws Exception {
            System.out.println("Starting HBase Test Cluster (in-memory)...");
            return utility.startMiniCluster();
        }
    }

    @Before
    public void setup() throws Exception {
        try {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(new ClusterStarter(utility)).get(CLUSTER_START_TIMEOUT, TimeUnit.SECONDS);
            configuration = utility.getConfiguration();
            createTables();
            createDAOs();
        } catch (TimeoutException tox) {
            fail("In-memory HBase Test Cluster could not be started in " + CLUSTER_START_TIMEOUT + " seconds - aborted execution of DAO-related test cases");
        }
    }

    private void createDAOs() throws IOException {
        citizenDao = new CitizenDAO(configuration);
        citizenSummaryDAO = new CitizenSummaryDAO(configuration);
        versionlessDAO = new VersionlessDAO(configuration);
    }

    private void createTables() throws IOException {
        utility.createTable("citizens".getBytes(), new byte[][]{"main".getBytes(), "optional".getBytes()});
        utility.createTable("citizen_summary".getBytes(), new byte[][]{"a".getBytes()});
        utility.createTable("test_history".getBytes(), new byte[][]{"f".getBytes()}, 3);
    }

    public void testTableParticulars() {
        assertEquals(citizenDao.getTableName(), "citizens");
        assertEquals(citizenSummaryDAO.getTableName(), "citizen_summary");
        assertTrue(TestUtil.setEquals(citizenDao.getColumnFamilies(), Sets.newHashSet("main", "optional")));
        assertTrue(TestUtil.setEquals(citizenSummaryDAO.getColumnFamilies(), Sets.newHashSet("a")));
    }

    public void testHBaseDAO() throws Exception {
        String[] rowKeys = new String[testObjs.size()];
        Map<String, Map<String, Object>> expectedFieldValues = new HashMap<String, Map<String, Object>>();
        for (int i = 0; i < testObjs.size(); i++) {
            Citizen e = testObjs.get(i);
            final String rowKey = citizenDao.persist(e);
            rowKeys[i] = rowKey;
            Citizen pe = citizenDao.get(rowKey);
            assertEquals("Entry got corrupted upon persisting and fetching back", e, pe);
            for (String f : citizenDao.getFields()) {
                Field field = Citizen.class.getDeclaredField(f);
                field.setAccessible(true);
                final Object actual = citizenDao.fetchFieldValue(rowKey, f);
                assertEquals("Field data corrupted upon persisting and fetching back", field.get(e), actual);
                if (actual == null) continue;
                if (!expectedFieldValues.containsKey(f)) {
                    expectedFieldValues.put(f, new HashMap<String, Object>() {
                        {
                            put(rowKey, actual);
                        }
                    });
                } else {
                    expectedFieldValues.get(f).put(rowKey, actual);
                }
            }
        }
        List<Citizen> citizens = citizenDao.get(rowKeys[0], rowKeys[rowKeys.length - 1]);
        for (int i = 0; i < citizens.size(); i++) {
            assertEquals("When retrieved in bulk (range scan), we have unexpected entry", citizens.get(i), testObjs.get(i));
        }
        for (String f : citizenDao.getFields()) {
            Map<String, Object> actualFieldValues = citizenDao.fetchFieldValues(rowKeys, f);
            Map<String, Object> actualFieldValuesScanned = citizenDao.fetchFieldValues("A", "z", f);
            assertTrue(String.format("Invalid data returned when values for column \"%s\" were fetched in bulk\nExpected: %s\nActual: %s", f, expectedFieldValues.get(f), actualFieldValues), TestUtil.mapEquals(actualFieldValues, expectedFieldValues.get(f)));
            assertTrue("Difference between 'bulk fetch by array of row keys' and 'bulk fetch by range of row keys'", TestUtil.mapEquals(actualFieldValues, actualFieldValuesScanned));
        }
        Map<String, Object> actualSalaries = citizenDao.fetchFieldValues(rowKeys, "sal");
        long actualSumOfSalaries = 0;
        for (Object s : actualSalaries.values()) {
            actualSumOfSalaries += s == null ? 0 : (Integer) s;
        }
        long expectedSumOfSalaries = 0;
        for (Citizen c : testObjs) {
            expectedSumOfSalaries += c.getSal() == null ? 0 : c.getSal();
        }
        assertEquals(expectedSumOfSalaries, actualSumOfSalaries);
        assertArrayEquals("Data mismatch between single and bulk 'get' calls", testObjs.toArray(), citizenDao.get(rowKeys));
        assertEquals("Data mismatch between List and array bulk variants of 'get' calls", testObjs, citizenDao.get(Arrays.asList(rowKeys)));
        Citizen citizenToBeDeleted = testObjs.get(0);
        citizenDao.delete(citizenToBeDeleted);
        assertNull("Record was not deleted: " + citizenToBeDeleted, citizenDao.get(citizenToBeDeleted.composeRowKey()));
        Citizen[] citizensToBeDeleted = new Citizen[]{testObjs.get(1), testObjs.get(2)};
        citizenDao.delete(citizensToBeDeleted);
        assertNull("Record was not deleted: " + citizensToBeDeleted[0], citizenDao.get(citizensToBeDeleted[0].composeRowKey()));
        assertNull("Record was not deleted: " + citizensToBeDeleted[1], citizenDao.get(citizensToBeDeleted[1].composeRowKey()));
    }

    @Test
    public void test() throws Exception {
        testTableParticulars();
        testHBaseDAO();
        testHBColumnMultiVersion();
    }

    public void testHBColumnMultiVersion() throws Exception {
        Double[] testNumbers = new Double[]{3.14159, 2.71828, 0.0};
        List<Versionless> list = new ArrayList<Versionless>();
        for (Double n : testNumbers) {
            list.add(new Versionless(n));
        }
        List<String> persistedKeys = versionlessDAO.persist(list);
        versionlessDAO.get(persistedKeys);
    }

    @After
    public void tearDown() throws Exception {
        utility.shutdownMiniCluster();
    }
}
