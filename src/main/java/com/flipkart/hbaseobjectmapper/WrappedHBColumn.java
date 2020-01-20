package com.flipkart.hbaseobjectmapper;


import com.flipkart.hbaseobjectmapper.annotations.Flag;
import com.flipkart.hbaseobjectmapper.annotations.HBColumn;
import com.flipkart.hbaseobjectmapper.annotations.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.exceptions.BothHBColumnAnnotationsPresentException;
import com.flipkart.hbaseobjectmapper.exceptions.DuplicateCodecFlagForColumnException;
import com.flipkart.hbaseobjectmapper.exceptions.FieldNotMappedToHBaseColumnException;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;


/**
 * A wrapper class for {@link HBColumn} and {@link HBColumnMultiVersion} annotations (for internal use only)
 */
class WrappedHBColumn {

    /**
     * 列族、列
     */
    private final String family, column;
    /**
     * 单版本、多版本
     */
    private final boolean multiVersioned, singleVersioned;
    /**
     * HBColumn或HBColumnMultiVersion 对应的字节码
     */
    private final Class<? extends Annotation> annotationClass;
    /**
     * 额外配置信息
     */
    private final Map<String, String> codecFlags;
    /**
     * 属性
     */
    private final Field field;

    /**
     * 对属性进行包装
     * @param field
     */
    WrappedHBColumn(Field field) {
        this(field, false);
    }

    /**
     * 对属性进行包装
     * @param field 属性
     * @param throwExceptionIfNonHBColumn 当类未包含 注解 HBColumn 和 HBColumnMultiVersion 是否抛出异常
     */
    @SuppressWarnings("unchecked")
    WrappedHBColumn(Field field, boolean throwExceptionIfNonHBColumn) {
        this.field = field;
        //列注解
        HBColumn hbColumn = field.getAnnotation(HBColumn.class);
        //列多版本注解
        HBColumnMultiVersion hbColumnMultiVersion = field.getAnnotation(HBColumnMultiVersion.class);

        //同一个属性字段，不能指定两种类型的列注解（即 HBColumn 和 HBColumnMultiVersion 不能同时使用）
        if (hbColumn != null && hbColumnMultiVersion != null) {
            throw new BothHBColumnAnnotationsPresentException(field);
        }
        if (hbColumn != null) {
            family = hbColumn.family();
            column = hbColumn.column();
            singleVersioned = true;
            multiVersioned = false;
            annotationClass = HBColumn.class;
            codecFlags = toMap(hbColumn.codecFlags());
        } else if (hbColumnMultiVersion != null) {
            family = hbColumnMultiVersion.family();
            column = hbColumnMultiVersion.column();
            singleVersioned = false;
            multiVersioned = true;
            annotationClass = HBColumnMultiVersion.class;
            codecFlags = toMap(hbColumnMultiVersion.codecFlags());
        } else {
            if (throwExceptionIfNonHBColumn) {
                throw new FieldNotMappedToHBaseColumnException(field.getDeclaringClass(), field.getName());
            }
            family = null;
            column = null;
            singleVersioned = false;
            multiVersioned = false;
            annotationClass = null;
            codecFlags = null;
        }
    }

    /**
     * 编码配置（针对特殊入参）
     * @param codecFlags
     * @return 转化为Map的结果
     */
    private Map<String, String> toMap(Flag[] codecFlags) {
        Map<String, String> flagsMap = new HashMap<>(codecFlags.length, 1.0f);
        for (Flag flag : codecFlags) {
            //不可配置重复的信息
            String previousValue = flagsMap.put(flag.name(), flag.value());
            if (previousValue != null) {
                throw new DuplicateCodecFlagForColumnException(field.getDeclaringClass(), field.getName(), annotationClass, flag.name());
            }
        }
        return flagsMap;
    }

    public String family() {
        return family;
    }

    public byte[] familyBytes() {
        return Bytes.toBytes(family);
    }

    public String column() {
        return column;
    }

    public byte[] columnBytes() {
        return Bytes.toBytes(column);
    }

    public Map<String, String> codecFlags() {
        return codecFlags;
    }

    /**
     * 版本信息是否存在
     * @return
     */
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

    @Override
    public String toString() {
        return String.format("%s:%s", family, column);
    }
}
