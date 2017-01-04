package com.flipkart.hbaseobjectmapper;


import com.flipkart.hbaseobjectmapper.exceptions.BothHBColumnAnnotationsPresentException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * A wrapper class for {@link HBColumn} and {@link HBColumnMultiVersion} annotations
 */
class WrappedHBColumn {
    private String family, column;
    private boolean multiVersioned = false, singleVersioned = false;
    private Class annotationClass;
    private Map<String, String> flags;

    WrappedHBColumn(Field field) {
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        HBColumnMultiVersion hbColumnMultiVersion = field.getAnnotation(HBColumnMultiVersion.class);
        if (hbColumn != null && hbColumnMultiVersion != null) {
            throw new BothHBColumnAnnotationsPresentException(field);
        }
        if (hbColumn != null) {
            family = hbColumn.family();
            column = hbColumn.column();
            singleVersioned = true;
            annotationClass = HBColumn.class;
            flags = toMap(hbColumn.codecFlags());
        } else if (hbColumnMultiVersion != null) {
            family = hbColumnMultiVersion.family();
            column = hbColumnMultiVersion.column();
            multiVersioned = true;
            annotationClass = HBColumnMultiVersion.class;
            flags = toMap(hbColumnMultiVersion.codecFlags());
        }
    }

    private Map<String, String> toMap(Flag[] flags) {
        Map<String, String> flagsMap = new HashMap<>();
        for (Flag flag : flags) {
            flagsMap.put(flag.name(), flag.value());
        }
        return flagsMap;
    }

    public String family() {
        return family;
    }

    public String column() {
        return column;
    }

    public Map<String, String> codecFlags() {
        return flags;
    }

    public boolean isPresent() {
        return singleVersioned || multiVersioned;
    }

    public boolean isMultiVersioned() {
        return multiVersioned;
    }

    public boolean isSingleVersioned() {
        return singleVersioned;
    }

    public String getName() {
        return annotationClass.getName();
    }
}
