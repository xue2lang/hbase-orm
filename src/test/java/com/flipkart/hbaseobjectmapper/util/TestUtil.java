package com.flipkart.hbaseobjectmapper.util;

import com.flipkart.hbaseobjectmapper.HBRecord;
import com.google.common.collect.Sets;
import org.javatuples.Triplet;

import java.util.Map;
import java.util.Set;

public class TestUtil {

    public static <T> boolean setEquals(Set<T> leftSet, Set<T> rightSet) {
        return !(leftSet == null || rightSet == null || leftSet.size() != rightSet.size()) && rightSet.containsAll(leftSet);
    }

    public static <K, V> boolean mapEquals(Map<K, V> leftMap, Map<K, V> rightMap) {
        if (leftMap == null || rightMap == null || leftMap.size() != rightMap.size()) return false;
        for (K key : leftMap.keySet()) {
            V value1 = leftMap.get(key);
            V value2 = rightMap.get(key);
            if (!value1.equals(value2)) return false;
        }
        return true;
    }

    public static Triplet<HBRecord, String, Class<? extends IllegalArgumentException>> triplet(HBRecord record, String classDescription, Class<? extends IllegalArgumentException> clazz) {
        return new Triplet<HBRecord, String, Class<? extends IllegalArgumentException>>(record, classDescription, clazz);
    }

    @SafeVarargs
    public static <T> T[] a(T... a) {
        return a;
    }

    @SafeVarargs
    public static <T> Set<T> s(T... a) {
        return Sets.newHashSet(a);
    }
}
