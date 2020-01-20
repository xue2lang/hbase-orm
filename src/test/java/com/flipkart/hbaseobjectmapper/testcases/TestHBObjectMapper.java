package com.flipkart.hbaseobjectmapper.testcases;

import static com.flipkart.hbaseobjectmapper.testcases.TestObjects.validObjects;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.annotations.Family;
import com.flipkart.hbaseobjectmapper.annotations.HBColumn;
import com.flipkart.hbaseobjectmapper.annotations.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.annotations.HBRowKey;
import com.flipkart.hbaseobjectmapper.annotations.HBTable;
import com.flipkart.hbaseobjectmapper.exceptions.EmptyConstructorInaccessibleException;
import com.flipkart.hbaseobjectmapper.exceptions.FieldsMappedToSameColumnException;
import com.flipkart.hbaseobjectmapper.exceptions.ImproperHBTableAnnotationExceptions;
import com.flipkart.hbaseobjectmapper.exceptions.IncompatibleFieldForHBColumnMultiVersionAnnotationException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBePrimitiveException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBeStaticException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBeTransientException;
import com.flipkart.hbaseobjectmapper.exceptions.MissingHBColumnFieldsException;
import com.flipkart.hbaseobjectmapper.exceptions.MissingHBRowKeyFieldsException;
import com.flipkart.hbaseobjectmapper.exceptions.NoEmptyConstructorException;
import com.flipkart.hbaseobjectmapper.exceptions.ObjectNotInstantiatableException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeComposedException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeEmptyException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCouldNotBeParsedException;
import com.flipkart.hbaseobjectmapper.testcases.entities.Citizen;
import com.flipkart.hbaseobjectmapper.testcases.entities.CitizenSummary;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassWithBadAnnotationStatic;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassWithBadAnnotationTransient;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassWithNoEmptyConstructor;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassWithNoHBColumns;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassWithNoHBRowKeys;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassWithPrimitives;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassWithTwoFieldsMappedToSameColumn;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassesWithFieldIncompatibleWithHBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.testcases.entities.ClassesWithInvalidHBTableAnnotation;
import com.flipkart.hbaseobjectmapper.testcases.entities.Crawl;
import com.flipkart.hbaseobjectmapper.testcases.entities.CrawlNoVersion;
import com.flipkart.hbaseobjectmapper.testcases.entities.Employee;
import com.flipkart.hbaseobjectmapper.testcases.entities.EmployeeSummary;
import com.flipkart.hbaseobjectmapper.testcases.entities.Singleton;
import com.flipkart.hbaseobjectmapper.testcases.entities.UninstantiatableClass;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Triple;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class TestHBObjectMapper {
    public static final int NUM_ITERATIONS = 100;
    @SuppressWarnings("unchecked")
    private final List<Triple<HBRecord, String, Class<? extends IllegalArgumentException>>> invalidRecordsAndErrorMessages = Arrays.asList(
            triple(Singleton.getInstance(), "A singleton class", EmptyConstructorInaccessibleException.class),
            triple(new ClassWithNoEmptyConstructor(1), "Class with no empty constructor", NoEmptyConstructorException.class),
            triple(new ClassWithPrimitives(1f), "A class with primitives", MappedColumnCantBePrimitiveException.class),
            triple(new ClassWithTwoFieldsMappedToSameColumn(), "Class with two fields mapped to same column", FieldsMappedToSameColumnException.class),
            triple(new ClassWithBadAnnotationStatic(), "Class with a static field mapped to HBase column", MappedColumnCantBeStaticException.class),
            triple(new ClassWithBadAnnotationTransient("James", "Gosling"), "Class with a transient field mapped to HBase column", MappedColumnCantBeTransientException.class),
            triple(new ClassWithNoHBColumns(), "Class with no fields mapped with HBColumn", MissingHBColumnFieldsException.class),
            triple(new ClassWithNoHBRowKeys(), "Class with no fields mapped with HBRowKey", MissingHBRowKeyFieldsException.class),
            triple(new ClassesWithFieldIncompatibleWithHBColumnMultiVersion.NotMap(), "Class with an incompatible field (not Map) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triple(new ClassesWithFieldIncompatibleWithHBColumnMultiVersion.NotNavigableMap(), "Class with an incompatible field (not NavigableMap) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triple(new ClassesWithFieldIncompatibleWithHBColumnMultiVersion.EntryKeyNotLong(), "Class with an incompatible field (NavigableMap's entry key not Long) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.InvalidVersions(), "Class with an invalid number of versions in it's HBTable annotation", ImproperHBTableAnnotationExceptions.InvalidValueForVersionsOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.EmptyTableName(), "Class with empty table name in it's HBTable annotation", ImproperHBTableAnnotationExceptions.EmptyTableNameOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.EmptyColumnFamily(), "Class with empty column family name in it's HBTable annotation", ImproperHBTableAnnotationExceptions.EmptyColumnFamilyOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.DuplicateColumnFamilies(), "Class with duplicate column families in it's HBTable annotation", ImproperHBTableAnnotationExceptions.DuplicateColumnFamilyNamesOnHBTableAnnotationException.class),
            triple(new ClassesWithInvalidHBTableAnnotation.MissingHBTableAnnotation(), "Class with no HBTable annotation", ImproperHBTableAnnotationExceptions.MissingHBTableAnnotationException.class)
    );

    private static Triple<HBRecord, String, Class<? extends IllegalArgumentException>> triple(HBRecord record, String message, Class<? extends IllegalArgumentException> exceptionClass) {
        return Triple.create(record, message, exceptionClass);
    }

    final HBObjectMapper hbMapper = new HBObjectMapper();

    final Result someResult = hbMapper.writeValueAsResult(validObjects.get(0));
    final Put somePut = hbMapper.writeValueAsPut(validObjects.get(0));

    @Test
    public void testHBObjectMapper() {
        for (HBRecord obj : validObjects) {
            System.out.printf("Original object: %s%n", obj);
            //测试序列化、反序列化耗时
            testResult(obj);
            //测试行数据 序列化、反序列化
            testResultWithRow(obj);
            //测试put数据 序列化、反序列化
            testPut(obj);
            //测试put行数据 序列化、反序列化
            testPutWithRow(obj);
            System.out.printf("*****%n%n");
        }
    }

    private void testResult(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Result result = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            result = hbMapper.writeValueAsResult(p);
        }
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO -> Result = %.2fms%n", timeTaken(start, end));
        start = System.currentTimeMillis();
        HBRecord pFromResult = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromResult = hbMapper.readValue(result, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Result", p, pFromResult);
        System.out.printf("Time taken for Result -> POJO = %.2fms%n", timeTaken(start, end));
    }

    private double timeTaken(long start, long end) {
        return (double) (end - start) / (double) NUM_ITERATIONS;
    }

    private <R extends Serializable & Comparable<R>> void testResultWithRow(HBRecord<R> p) {
        long start, end;
        Result result = hbMapper.writeValueAsResult(l(p, p)).get(0);
        ImmutableBytesWritable rowKey = hbMapper.getRowKey(p);
        start = System.currentTimeMillis();
        HBRecord pFromResult = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromResult = hbMapper.readValue(rowKey, result, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Result+Row", p, pFromResult);
        System.out.printf("Time taken for Rowkey+Result -> POJO = %.2fms%n", timeTaken(start, end));
    }

    @SafeVarargs
    private final <R extends Serializable & Comparable<R>> List<HBRecord<R>> l(HBRecord<R>... records) {
        ArrayList<HBRecord<R>> list = new ArrayList<>();
        Collections.addAll(list, records);
        return list;

    }

    private void testPut(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Put put = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            put = (Put) hbMapper.writeValueAsPut(l(p, p)).get(0);
        }
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO -> Put = %.2fms%n", timeTaken(start, end));
        start = System.currentTimeMillis();
        HBRecord pFromPut = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromPut = hbMapper.readValue(put, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Put -> POJO = %.2fms%n", timeTaken(start, end));
    }

    private <R extends Serializable & Comparable<R>> void testPutWithRow(HBRecord<R> p) {
        long start, end;
        Put put = hbMapper.writeValueAsPut(p);
        ImmutableBytesWritable rowKey = hbMapper.getRowKey(p);
        start = System.currentTimeMillis();
        HBRecord pFromPut = null;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            pFromPut = hbMapper.readValue(rowKey, put, p.getClass());
        }
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Rowkey+Put -> POJO = %.2fms%n%n", timeTaken(start, end));
    }

    @Test(expected = RowKeyCouldNotBeParsedException.class)
    public void testInvalidRowKey() {
        hbMapper.readValue(hbMapper.toIbw("invalid row key"), hbMapper.writeValueAsPut(validObjects.get(0)), Citizen.class);
    }

    @Test
    public void testValidClasses() {
        for (Class clazz : Arrays.asList(Citizen.class, CitizenSummary.class, Employee.class, EmployeeSummary.class)) {
            assertTrue(hbMapper.isValid(clazz));
        }
    }

    @Test
    public void testInvalidClasses() {
        final String ERROR_MESSAGE = "Mismatch in type of exception thrown for class ";
        Set<String> exceptionMessages = new HashSet<>();
        for (Triple<HBRecord, String, Class<? extends IllegalArgumentException>> p : invalidRecordsAndErrorMessages) {
            HBRecord record = p.getFirst();
            Class recordClass = record.getClass();
            assertFalse("Object mapper couldn't detect issue with invalid class " + recordClass.getName(), hbMapper.isValid(recordClass));
            String errorMessage = p.getSecond() + " (" + recordClass.getName() + ") should have thrown an " + IllegalArgumentException.class.getName();
            String exMsgObjToResult = null, exMsgObjToPut = null, exMsgResultToObj = null, exMsgPutToObj = null;
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result");
            } catch (IllegalArgumentException ex) {
                assertEquals(ERROR_MESSAGE + recordClass.getSimpleName(), p.getThird(), ex.getClass());
                exMsgObjToResult = ex.getMessage();
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put");
            } catch (IllegalArgumentException ex) {
                assertEquals(ERROR_MESSAGE + recordClass.getSimpleName(), p.getThird(), ex.getClass());
                exMsgObjToPut = ex.getMessage();
            }
            try {
                hbMapper.readValue(someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(ERROR_MESSAGE + recordClass.getSimpleName(), p.getThird(), ex.getClass());
                exMsgResultToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(someResult.getRow()), someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(ERROR_MESSAGE + recordClass.getSimpleName(), p.getThird(), ex.getClass());
            }
            try {
                hbMapper.readValue(somePut, recordClass);
                fail(errorMessage + " while converting Put to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(ERROR_MESSAGE + recordClass.getSimpleName(), p.getThird(), ex.getClass());
                exMsgPutToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(somePut.getRow()), somePut, recordClass);
                fail(errorMessage + " while converting row key and Put combo to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals(ERROR_MESSAGE + recordClass.getSimpleName(), p.getThird(), ex.getClass());
            }
            assertEquals("Validation for 'conversion to Result' and 'conversion to Put' differ in code path", exMsgObjToResult, exMsgObjToPut);
            assertEquals("Validation for 'conversion from Result' and 'conversion from Put' differ in code path", exMsgResultToObj, exMsgPutToObj);
            assertEquals("Validation for 'conversion from bean' and 'conversion to bean' differ in code path", exMsgObjToResult, exMsgResultToObj);
            System.out.printf("[edge case] %s threw below Exception, as expected:%n%s%n%n", p.getSecond(), exMsgObjToResult);
            assertTrue("Same error message for different invalid inputs", exceptionMessages.add(exMsgObjToPut));
        }
    }

    @Test
    public void testInvalidObjects() {
        for (Triple<HBRecord, String, Class<? extends IllegalArgumentException>> p : TestObjects.invalidObjects) {
            HBRecord record = p.getFirst();
            String errorMessage = "An object with " + p.getSecond() + " should've thrown an " + p.getThird().getName();
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result\nFailing object = " + record);
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getThird(), ex.getClass());
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put\nFailing object = " + record);
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getThird(), ex.getClass());
            }
        }
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEmptyResults() {
        Result nullResult = null, blankResult = new Result(), emptyResult = Result.EMPTY_RESULT;
        Citizen nullCitizen = hbMapper.readValue(nullResult, Citizen.class);
        assertNull("Null Result object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(blankResult, Citizen.class);
        assertNull("Empty Result object should return null", emptyCitizen);
        assertNull(hbMapper.readValue(emptyResult, Citizen.class));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEmptyPuts() {
        Put nullPut = null;
        Citizen nullCitizen = hbMapper.readValue(nullPut, Citizen.class);
        assertNull("Null Put object should return null", nullCitizen);
    }

    @HBTable(name = "dummy1", families = {@Family(name = "a")})
    public static class DummyRowKeyClass implements HBRecord<String> {
        @HBRowKey
        private String rowKey;

        @HBColumn(family = "a", column = "d")
        private String dummy;

        public DummyRowKeyClass() {

        }

        public DummyRowKeyClass(String rowKey) {
            this.rowKey = rowKey;
        }

        @Override
        public String composeRowKey() {
            return rowKey;
        }

        @Override
        public void parseRowKey(String rowKey) {
            this.rowKey = rowKey;
        }
    }

    @HBTable(name = "dummy2", families = {@Family(name = "a")})
    public static class RowKeyComposeThrowsExceptionClass implements HBRecord<String> {
        @HBRowKey
        private String rowKey;

        @HBColumn(family = "a", column = "d")
        private String dummy;

        @Override
        public String composeRowKey() {
            throw new RuntimeException("Some blah");
        }

        @Override
        public void parseRowKey(String rowKey) {

        }
    }

    @Test
    public void testGetRowKey() {
        assertEquals("Row keys don't match", hbMapper.getRowKey(new DummyRowKeyClass("rowkey")), hbMapper.toIbw("rowkey"));
        try {
            hbMapper.getRowKey(new DummyRowKeyClass(null));
            fail("null row key should've thrown a " + RowKeyCantBeEmptyException.class.getName());
        } catch (RowKeyCantBeEmptyException ignored) {

        }
        try {
            hbMapper.getRowKey(new RowKeyComposeThrowsExceptionClass());
            fail("If row key can't be composed, an " + RowKeyCantBeComposedException.class.getName() + " was expected");
        } catch (RowKeyCantBeComposedException ignored) {

        }
        try {
            HBRecord<Integer> nullRecord = null;
            hbMapper.getRowKey(nullRecord);
            fail("If object is null, a " + NullPointerException.class.getSimpleName() + " was expected");
        } catch (NullPointerException ignored) {

        }
    }

    @Test
    public void testUninstantiatableClass() {
        try {
            hbMapper.readValue(someResult, UninstantiatableClass.class);
            fail("If class can't be instantiated, a " + ObjectNotInstantiatableException.class.getName() + " was expected");
        } catch (ObjectNotInstantiatableException ignored) {
        }
    }

    @Test
    public void testHBColumnMultiVersion() {
        Double[] testNumbers = new Double[]{3.14159, 2.71828, 0.0};
        for (Double n : testNumbers) {
            // Written as unversioned, read as versioned
            Result result = hbMapper.writeValueAsResult(new CrawlNoVersion("key").setF1(n));
            Crawl versioned = hbMapper.readValue(result, Crawl.class);
            NavigableMap<Long, Double> columnHistory = versioned.getF1();
            assertEquals("Column history size mismatch", 1, columnHistory.size());
            assertEquals(String.format("Inconsistency between %s and %s", HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()), n, columnHistory.lastEntry().getValue());
            // Written as versioned, read as unversioned
            Crawl key = new Crawl("key").addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE);
            Crawl versionedCrawl = key.addF1(n);
            Result result1 = hbMapper.writeValueAsResult(versionedCrawl);
            CrawlNoVersion unversionedCrawl = hbMapper.readValue(result1, CrawlNoVersion.class);
            Double f1 = unversionedCrawl.getF1();
            System.out.println(unversionedCrawl);
            assertEquals(String.format("Inconsistency between %s and %s\nVersioned (persisted) object = %s\nUnversioned (retrieved) object = %s ", HBColumnMultiVersion.class.getSimpleName(), HBColumn.class.getSimpleName(), versionedCrawl, unversionedCrawl), n, f1);
        }
    }
}
