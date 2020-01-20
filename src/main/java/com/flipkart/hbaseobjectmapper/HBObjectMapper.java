package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.annotations.HBRowKey;
import com.flipkart.hbaseobjectmapper.annotations.HBTable;
import com.flipkart.hbaseobjectmapper.annotations.MappedSuperClass;
import com.flipkart.hbaseobjectmapper.codec.BestSuitCodec;
import com.flipkart.hbaseobjectmapper.codec.Codec;
import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.exceptions.SerializationException;
import com.flipkart.hbaseobjectmapper.exceptions.InternalError;
import com.flipkart.hbaseobjectmapper.exceptions.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;

/**
 * <p>An <b>object mapper class</b> that helps<ol>
 * <li>serialize objects of your <i>bean-like class</i> to HBase's {@link Put} and {@link Result} objects</li>
 * <li>deserialize HBase's {@link Put} and {@link Result} objects to objects of your <i>bean-like class</i></li>
 * </ol>
 * where, your <i>bean-like class</i> is like any other POJO/bean but implements {@link HBRecord} interface.<br>
 * <br>
 * <p>
 * This class is for use in:<ol>
 * <li>MapReduce jobs which <i>read from</i> and/or <i>write to</i> HBase tables</li>
 * <li>Unit-tests</li>
 * </ol>
 * <p><b>This class is thread-safe.</b> This class is designed in a way that only one instance needs to be maintained for the entire lifecycle of your program
 *
 * @see <a href="https://en.wikipedia.org/wiki/Plain_old_Java_object">POJO</a>
 * @see <a href="https://en.wikipedia.org/wiki/JavaBeans">JavaBeans</a>
 */
public class HBObjectMapper {

    private static final Codec DEFAULT_CODEC = new BestSuitCodec();

    private final Codec codec;

    /**
     * Instantiate object of this class with a custom {@link Codec}
     *
     * @param codec Codec to be used for serialization and deserialization of fields
     * @see #HBObjectMapper()
     */
    public HBObjectMapper(Codec codec) {
        if (codec == null) {
            throw new IllegalArgumentException("Parameter 'codec' cannot be null. If you want to use the default codec, use the no-arg constructor");
        }
        this.codec = codec;
    }

    /**
     * Instantiate an object of this class with default {@link Codec} (that is, {@link BestSuitCodec})
     *
     * @see #HBObjectMapper(Codec)
     */
    public HBObjectMapper() {
        this(DEFAULT_CODEC);
    }

    /**
     * Serialize row key
     *
     * @param rowKey Object representing row key
     * @param <R>    Data type of row key
     * @return Byte array
     */
    <R extends Serializable & Comparable<R>> byte[] rowKeyToBytes(R rowKey, Map<String, String> codecFlags) {
        return valueToByteArray(rowKey, codecFlags);
    }

    @SuppressWarnings("unchecked")
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> R bytesToRowKey(byte[] rowKeyBytes, Map<String, String> codecFlags, Class<T> entityClass) {
        try {
            return (R) byteArrayToValue(rowKeyBytes, entityClass.getDeclaredMethod("composeRowKey").getReturnType(), codecFlags);
        } catch (NoSuchMethodException e) {
            throw new InternalError(e);
        }
    }

    /**
     * Core method that drives deserialization
     *
     * @see #convertRecordToMap(HBRecord)
     */
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T convertMapToRecord(byte[] rowKeyBytes, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map, Class<T> clazz) {
        Collection<Field> fields = getHBColumnFields0(clazz).values();
        WrappedHBTable<R, T> hbTable = new WrappedHBTable<>(clazz);
        R rowKey = bytesToRowKey(rowKeyBytes, hbTable.getCodecFlags(), clazz);
        T record;
        try {
            record = clazz.getDeclaredConstructor()
                    .newInstance();
        } catch (Exception ex) {
            throw new ObjectNotInstantiatableException("Error while instantiating empty constructor of " + clazz.getName(), ex);
        }
        try {
            record.parseRowKey(rowKey);
        } catch (Exception ex) {
            throw new RowKeyCouldNotBeParsedException(String.format("Supplied row key \"%s\" could not be parsed", rowKey), ex);
        }
        for (Field field : fields) {
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(hbColumn.familyBytes());
            if (familyMap == null || familyMap.isEmpty()) {
                continue;
            }
            NavigableMap<Long, byte[]> columnVersionsMap = familyMap.get(hbColumn.columnBytes());
            if (hbColumn.isSingleVersioned()) {
                if (columnVersionsMap == null || columnVersionsMap.isEmpty()) {
                    continue;
                }
                Map.Entry<Long, byte[]> lastEntry = columnVersionsMap.lastEntry();
                objectSetFieldValue(record, field, lastEntry.getValue(), hbColumn.codecFlags());
            } else {
                objectSetFieldValue(record, field, columnVersionsMap, hbColumn.codecFlags());
            }
        }
        return record;
    }

    /**
     * Converts a {@link Serializable} object into a <code>byte[]</code>
     *
     * @param value      Object to be serialized
     * @param codecFlags Flags to be passed to Codec
     * @return Byte-array representing serialized object
     * @see #byteArrayToValue(byte[], Type, Map)
     */
    byte[] valueToByteArray(Serializable value, Map<String, String> codecFlags) {
        try {
            return codec.serialize(value, codecFlags);
        } catch (SerializationException e) {
            throw new CodecException("Couldn't serialize", e);
        }
    }

    /**
     * <p>Serialize an object to HBase's {@link ImmutableBytesWritable}.
     * <p>This method is for use in Mappers, unit-tests for Mappers and unit-tests for Reducers.
     *
     * @param value Object to be serialized
     * @return Byte array, wrapped in HBase's data type
     * @see #getRowKey
     */
    public ImmutableBytesWritable toIbw(Serializable value) {
        return new ImmutableBytesWritable(valueToByteArray(value, null));
    }

    /**
     * 获取表的信息（返回包装类）
     * @param clazz 实体类
     * @param <R> 行
     * @param <T> 返回值
     * @return
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> WrappedHBTable<R, T> validateHBClass(Class<T> clazz) {
        Constructor<?> constructor;
        try {
            //获取无参构造
            constructor = clazz.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new NoEmptyConstructorException(clazz, e);
        }
        //判断是否私有
        if (!Modifier.isPublic(constructor.getModifiers())) {
            throw new EmptyConstructorInaccessibleException(String.format("Empty constructor of class %s is inaccessible. It needs to be public.", clazz.getName()));
        }
        int numOfHBColumns = 0, numOfHBRowKeys = 0;
        //表的包装类
        WrappedHBTable<R, T> hbTable = new WrappedHBTable<>(clazz);
        //获取属性（绑定列族、列的唯一标识）
        Set<FamilyAndColumn> columns = new HashSet<>(clazz.getDeclaredFields().length, 1.0f);
        for (Field field : clazz.getDeclaredFields()) {
            //rowKey信息
            if (field.isAnnotationPresent(HBRowKey.class)) {
                numOfHBRowKeys++;
            }
        }
        //实体类必须含有rowKey，否则抛出异常
        if (numOfHBRowKeys == 0) {
            throw new MissingHBRowKeyFieldsException(clazz);
        }
        Map<String, Field> hbColumnFields = getHBColumnFields0(clazz);
        for (Field field : hbColumnFields.values()) {
            //获取属性的包装对象
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            //存在版本信息
            if (hbColumn.isPresent()) {
                //判断是否配置表的列族信息
                if (!hbTable.isColumnFamilyPresent(hbColumn.family())) {
                    throw new IllegalArgumentException(String.format("Class %s has field '%s' mapped to HBase column '%s' - but column family '%s' isn't configured in @%s annotation",
                            clazz.getName(), field.getName(), hbColumn, hbColumn.family(), HBTable.class.getSimpleName()));
                }
                //单版本
                if (hbColumn.isSingleVersioned()) {
                    validateHBColumnSingleVersionField(field);
                } else if (hbColumn.isMultiVersioned()) {
                    //多版本
                    validateHBColumnMultiVersionField(field);
                }
                if (!columns.add(new FamilyAndColumn(hbColumn.family(), hbColumn.column()))) {
                    throw new FieldsMappedToSameColumnException(String.format("Class %s has more than one field (e.g. '%s') mapped to same HBase column %s", clazz.getName(), field.getName(), hbColumn));
                }
                numOfHBColumns++;
            }
        }
        if (numOfHBColumns == 0) {
            throw new MissingHBColumnFieldsException(clazz);
        }
        return hbTable;
    }

    /**
     * Internal note: This should be in sync with {@link #getFieldType(Field, boolean)}
     */
    private void validateHBColumnMultiVersionField(Field field) {
        validateHBColumnField(field);
        if (!(field.getGenericType() instanceof ParameterizedType)) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException(String.format("Field %s is not even a parameterized type", field));
        }
        if (field.getType() != NavigableMap.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException(String.format("Field %s is not a NavigableMap", field));
        }
        ParameterizedType pType = (ParameterizedType) field.getGenericType();
        Type[] typeArguments = pType.getActualTypeArguments();
        if (typeArguments.length != 2 || typeArguments[0] != Long.class) {
            throw new IncompatibleFieldForHBColumnMultiVersionAnnotationException(String.format("Field %s has unexpected type params (Key should be of %s type)", field, Long.class.getName()));
        }
        if (!codec.canDeserialize(getFieldType(field, true))) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type Navigable<Long,%s> ", field.getName(), field.getDeclaringClass().getName(), field.getDeclaringClass().getName()));
        }
    }

    /**
     * Internal note: For multi-version usecase, this should be in sync with {@link #validateHBColumnMultiVersionField(Field)}
     */
    Type getFieldType(Field field, boolean isMultiVersioned) {
        //多版本
        if (isMultiVersioned) {
            return ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
        } else {
            return field.getGenericType();
        }
    }

    /**
     * 校验单版本属性
     * @param field
     */
    private void validateHBColumnSingleVersionField(Field field) {
        //校验属性
        validateHBColumnField(field);
        //获取属性泛型类型
        Type fieldType = getFieldType(field, false);
        if (fieldType instanceof Class) {
            Class<?> fieldClazz = (Class<?>) fieldType;
            if (fieldClazz.isPrimitive()) {
                throw new MappedColumnCantBePrimitiveException(String.format("Field %s in class %s is a primitive of type %s (Primitive data types are not supported as they're not nullable)", field.getName(), field.getDeclaringClass().getName(), fieldClazz.getName()));
            }
        }
        //是否支持序列化
        if (!codec.canDeserialize(fieldType)) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type (%s)", field.getName(), field.getDeclaringClass().getName(), fieldType));
        }
    }

    /**
     * 校验列属性（标注@HBColumn 注解的属性，不能使用 transient、static修饰），否则抛出异常
     * @param field
     */
    private void validateHBColumnField(Field field) {
        WrappedHBColumn hbColumn = new WrappedHBColumn(field);
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers)) {
            throw new MappedColumnCantBeTransientException(field, hbColumn.getName());
        }
        if (Modifier.isStatic(modifiers)) {
            throw new MappedColumnCantBeStaticException(field, hbColumn.getName());
        }
    }

    /**
     * Core method that drives serialization
     *
     * @see #convertMapToRecord(byte[], NavigableMap, Class)
     */
    @SuppressWarnings("unchecked")
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> convertRecordToMap(HBRecord<R> record) {
        Class<T> clazz = (Class<T>) record.getClass();
        //得到所有的属性
        Collection<Field> fields = getHBColumnFields0(clazz).values();
        //排序
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        int numOfFieldsToWrite = 0;
        for (Field field : fields) {
            //列属性
            WrappedHBColumn hbColumn = new WrappedHBColumn(field);
            //单版本
            if (hbColumn.isSingleVersioned()) {
                byte[] familyName = hbColumn.familyBytes();
                byte[] columnName = hbColumn.columnBytes();
                if (!map.containsKey(familyName)) {
                    map.put(familyName, new TreeMap<>(Bytes.BYTES_COMPARATOR));
                }
                Map<byte[], NavigableMap<Long, byte[]>> columns = map.get(familyName);
                final byte[] fieldValueBytes = getFieldValueAsBytes(record, field, hbColumn.codecFlags());
                if (fieldValueBytes == null || fieldValueBytes.length == 0) {
                    continue;
                }
                NavigableMap<Long, byte[]> singleValue = new TreeMap<>();
                singleValue.put(HConstants.LATEST_TIMESTAMP, fieldValueBytes);
                columns.put(columnName, singleValue);
                numOfFieldsToWrite++;
            } else if (hbColumn.isMultiVersioned()) {
                NavigableMap<Long, byte[]> fieldValueVersions = getFieldValuesAsNavigableMapOfBytes(record, field, hbColumn.codecFlags());
                if (fieldValueVersions == null){
                    continue;
                }
                byte[] familyName = hbColumn.familyBytes(), columnName = hbColumn.columnBytes();
                if (!map.containsKey(familyName)) {
                    map.put(familyName, new TreeMap<>(Bytes.BYTES_COMPARATOR));
                }
                Map<byte[], NavigableMap<Long, byte[]>> columns = map.get(familyName);
                columns.put(columnName, fieldValueVersions);
                numOfFieldsToWrite++;
            }
        }
        if (numOfFieldsToWrite == 0) {
            throw new AllHBColumnFieldsNullException();
        }
        return map;
    }

    private <R extends Serializable & Comparable<R>> byte[] getFieldValueAsBytes(HBRecord<R> record, Field field, Map<String, String> codecFlags) {
        Serializable fieldValue;
        try {
            field.setAccessible(true);
            fieldValue = (Serializable) field.get(record);
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
        return valueToByteArray(fieldValue, codecFlags);
    }

    private <R extends Serializable & Comparable<R>> NavigableMap<Long, byte[]> getFieldValuesAsNavigableMapOfBytes(HBRecord<R> record, Field field, Map<String, String> codecFlags) {
        try {
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            NavigableMap<Long, R> fieldValueVersions = (NavigableMap<Long, R>) field.get(record);
            if (fieldValueVersions == null){
                return null;
            }
            if (fieldValueVersions.size() == 0) {
                throw new FieldAnnotatedWithHBColumnMultiVersionCantBeEmpty();
            }
            NavigableMap<Long, byte[]> output = new TreeMap<>();
            for (Map.Entry<Long, R> e : fieldValueVersions.entrySet()) {
                Long timestamp = e.getKey();
                R fieldValue = e.getValue();
                if (fieldValue == null){
                    continue;
                }
                byte[] fieldValueBytes = valueToByteArray(fieldValue, codecFlags);
                output.put(timestamp, fieldValueBytes);
            }
            return output;
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    /**
     * <p>Converts an object of your bean-like class to HBase's {@link Put} object.
     * <p>This method is for use in a MapReduce job whose <code>Reducer</code> class extends HBase's <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class (in other words, a MapReduce job whose output is an HBase table)
     *
     * @param record An object of your bean-like class (one that implements {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return HBase's {@link Put} object
     */
    @SuppressWarnings("unchecked")
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Put writeValueAsPut(HBRecord<R> record) {
        validateHBClass((Class<T>) record.getClass());
        //创建put实例
        Put put = new Put(composeRowKey(record));
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : convertRecordToMap(record).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> columnValuesVersioned = e.getValue();
                if (columnValuesVersioned == null){
                    continue;
                }
                //返回列信息
                for (Map.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                    put.addColumn(family, columnName, versionAndValue.getKey(), versionAndValue.getValue());
                }
            }
        }
        return put;
    }

    /**
     * A <i>bulk version</i> of {@link #writeValueAsPut(HBRecord)} method
     *
     * @param records List of objects of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>     Data type of row key
     * @return List of HBase's {@link Put} objects
     */
    public <R extends Serializable & Comparable<R>> List<Put> writeValueAsPut(List<HBRecord<R>> records) {
        List<Put> puts = new ArrayList<>(records.size());
        for (HBRecord<R> record : records) {
            Put put = writeValueAsPut(record);
            puts.add(put);
        }
        return puts;
    }

    /**
     * <p>Converts an object of your bean-like class to HBase's {@link Result} object.
     * <p>This method is for use in unit-tests of a MapReduce job whose <code>Mapper</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class (in other words, a MapReduce job whose input in an HBase table)
     *
     * @param record object of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return HBase's {@link Result} object
     */
    @SuppressWarnings("unchecked")
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Result writeValueAsResult(HBRecord<R> record) {
        validateHBClass((Class<T>) record.getClass());
        byte[] row = composeRowKey(record);
        List<Cell> cellList = new ArrayList<>();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : convertRecordToMap(record).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> valuesVersioned = e.getValue();
                if (valuesVersioned == null){
                    continue;
                }
                for (Map.Entry<Long, byte[]> columnVersion : valuesVersioned.entrySet()) {
                    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
                    cellBuilder.setType(Cell.Type.Put).setRow(row).setFamily(family).setQualifier(columnName).setTimestamp(columnVersion.getKey()).setValue(columnVersion.getValue());
                    Cell cell = cellBuilder.build();
                    cellList.add(cell);
                }
            }
        }
        return Result.create(cellList);
    }

    /**
     * A <i>bulk version</i> of {@link #writeValueAsResult(HBRecord)} method
     *
     * @param records List of objects of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>     Data type of row key
     * @return List of HBase's {@link Result} objects
     */
    public <R extends Serializable & Comparable<R>> List<Result> writeValueAsResult(List<HBRecord<R>> records) {
        List<Result> results = new ArrayList<>(records.size());
        for (HBRecord<R> record : records) {
            Result result = writeValueAsResult(record);
            results.add(result);
        }
        return results;
    }

    /**
     * <p>Converts HBase's {@link Result} object to an object of your bean-like class.
     * <p>This method is for use in a MapReduce job whose <code>Mapper</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class (in other words, a MapReduce job whose input is an HBase table)
     *
     * @param rowKey Row key of the record that corresponds to {@link Result}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Result}
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(ImmutableBytesWritable rowKey, Result result, Class<T> clazz) {
        validateHBClass(clazz);
        if (rowKey == null){
            return readValueFromResult(result, clazz);
        }
        return readValueFromRowAndResult(rowKey.get(), result, clazz);
    }

    /**
     * A compact version of {@link #readValue(ImmutableBytesWritable, Result, Class)} method
     *
     * @param result HBase's {@link Result} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Result result, Class<T> clazz) {
        validateHBClass(clazz);
        return readValueFromResult(result, clazz);
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(R rowKey, Result result, Class<T> clazz) {
        if (rowKey == null){
            return readValueFromResult(result, clazz);
        }
        return readValueFromRowAndResult(rowKeyToBytes(rowKey, WrappedHBTable.getCodecFlags(clazz)), result, clazz);
    }

    private boolean isResultEmpty(Result result) {
        return result == null || result.isEmpty() || result.getRow() == null || result.getRow().length == 0;
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromResult(Result result, Class<T> clazz) {
        if (isResultEmpty(result)){
            return null;
        }
        return convertMapToRecord(result.getRow(), result.getMap(), clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromRowAndResult(byte[] rowKeyBytes, Result result, Class<T> clazz) {
        if (isResultEmpty(result)) {
            return null;
        }
        return convertMapToRecord(rowKeyBytes, result.getMap(), clazz);
    }

    private void objectSetFieldValue(Object obj, Field field, NavigableMap<Long, byte[]> columnValuesVersioned, Map<String, String> codecFlags) {
        if (columnValuesVersioned == null){
            return;
        }
        try {
            field.setAccessible(true);
            NavigableMap<Long, Object> columnValuesVersionedBoxed = new TreeMap<>();
            for (Map.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                columnValuesVersionedBoxed.put(versionAndValue.getKey(), byteArrayToValue(versionAndValue.getValue(), ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1], codecFlags));
            }
            field.set(obj, columnValuesVersionedBoxed);
        } catch (Exception ex) {
            throw new ConversionFailedException(String.format("Could not set value on field \"%s\" on instance of class %s", field.getName(), obj.getClass()), ex);
        }
    }

    private void objectSetFieldValue(Object obj, Field field, byte[] value, Map<String, String> codecFlags) {
        if (value == null || value.length == 0){
            return;
        }
        try {
            field.setAccessible(true);
            field.set(obj, byteArrayToValue(value, field.getGenericType(), codecFlags));
        } catch (IllegalAccessException e) {
            throw new ConversionFailedException(String.format("Could not set value on field \"%s\" on instance of class %s", field.getName(), obj.getClass()), e);
        }
    }


    /**
     * Converts a byte array representing HBase column data to appropriate data type (boxed as object)
     *
     * @see #valueToByteArray(Serializable, Map)
     */
    Object byteArrayToValue(byte[] value, Type type, Map<String, String> codecFlags) {
        try {
            if (value == null || value.length == 0){
                return null;
            }
            return codec.deserialize(value, type, codecFlags);
        } catch (DeserializationException e) {
            throw new CodecException("Error while deserializing", e);
        }
    }

    /**
     * <p>Converts HBase's {@link Put} object to an object of your bean-like class
     * <p>This method is for use in unit-tests of a MapReduce job whose <code>Reducer</code> class extends <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class (in other words, a MapReduce job whose output is an HBase table)
     *
     * @param rowKey Row key of the record that corresponds to {@link Put}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Put} object
     * @param put    HBase's {@link Put} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(ImmutableBytesWritable rowKey, Put put, Class<T> clazz) {
        validateHBClass(clazz);
        if (rowKey == null){
            return readValueFromPut(put, clazz);
        }
       return readValueFromRowAndPut(rowKey.get(), put, clazz);
    }


    /**
     * A variant of {@link #readValue(ImmutableBytesWritable, Put, Class)} method
     *
     * @param rowKey Row key of the record that corresponds to {@link Put}. If this is <code>null</code>, an attempt will be made to resolve it from {@link Put} object
     * @param put    HBase's {@link Put} object
     * @param clazz  {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(R rowKey, Put put, Class<T> clazz) {
        if (rowKey == null) {
            return readValueFromPut(put, clazz);
        }
        return readValueFromRowAndPut(rowKeyToBytes(rowKey, WrappedHBTable.getCodecFlags(clazz)), put, clazz);

    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromRowAndPut(byte[] rowKeyBytes, Put put, Class<T> clazz) {
        Map<byte[], List<Cell>> rawMap = put.getFamilyCellMap();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], List<Cell>> familyNameAndColumnValues : rawMap.entrySet()) {
            byte[] family = familyNameAndColumnValues.getKey();
            if (!map.containsKey(family)) {
                map.put(family, new TreeMap<>(Bytes.BYTES_COMPARATOR));
            }
            List<Cell> cellList = familyNameAndColumnValues.getValue();
            for (Cell cell : cellList) {
                byte[] column = CellUtil.cloneQualifier(cell);
                if (!map.get(family).containsKey(column)) {
                    map.get(family).put(column, new TreeMap<>());
                }
                map.get(family).get(column).put(cell.getTimestamp(), CellUtil.cloneValue(cell));
            }
        }
        return convertMapToRecord(rowKeyBytes, map, clazz);
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValueFromPut(Put put, Class<T> clazz) {
        if (put == null || put.isEmpty() || put.getRow() == null || put.getRow().length == 0) {
            return null;
        }
        return readValueFromRowAndPut(put.getRow(), put, clazz);
    }

    /**
     * A compact version of {@link #readValue(ImmutableBytesWritable, Put, Class)} method
     *
     * @param put   HBase's {@link Put} object
     * @param clazz {@link Class} to which you want to convert to (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return Object of bean-like class
     * @throws CodecException One or more column values is a <code>byte[]</code> that couldn't be deserialized into field type (as defined in your entity class)
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Put put, Class<T> clazz) {
        validateHBClass(clazz);
        return readValueFromPut(put, clazz);
    }

    /**
     * Get row key (for use in HBase) from a bean-line object.<br>
     * For use in:
     * <ul>
     * <li>reducer jobs that extend HBase's <code>org.apache.hadoop.hbase.mapreduce.TableReducer</code> class</li>
     * <li>unit tests for mapper jobs that extend HBase's <code>org.apache.hadoop.hbase.mapreduce.TableMapper</code> class</li>
     * </ul>
     *
     * @param record object of your bean-like class (of type that extends {@link HBRecord})
     * @param <R>    Data type of row key
     * @param <T>    Entity type
     * @return Serialised row key wrapped in {@link ImmutableBytesWritable}
     * @see #toIbw(Serializable)
     */
    @SuppressWarnings("unchecked")
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> ImmutableBytesWritable getRowKey(HBRecord<R> record) {
        if (record == null) {
            throw new NullPointerException("Cannot compose row key for null objects");
        }
        validateHBClass((Class<T>) record.getClass());
        return new ImmutableBytesWritable(composeRowKey(record));
    }

    /**
     * 获取rowKey
     * @param record
     * @param <R>
     * @param <T>
     * @return
     */
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> byte[] composeRowKey(HBRecord<R> record) {
        R rowKey;
        try {
            rowKey = record.composeRowKey();
        } catch (Exception ex) {
            throw new RowKeyCantBeComposedException(ex);
        }
        if (rowKey == null || rowKey.toString().isEmpty()) {
            throw new RowKeyCantBeEmptyException();
        }
        @SuppressWarnings("unchecked")
        WrappedHBTable<R, T> hbTable = new WrappedHBTable<>((Class<T>) record.getClass());
        return valueToByteArray(rowKey, hbTable.getCodecFlags());
    }

    /**
     * Get list of column families and their max versions, mapped in definition of your bean-like class
     *
     * @param clazz {@link Class} that you're reading (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return Map of column families and their max versions
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Integer> getColumnFamiliesAndVersions(Class<T> clazz) {
        final WrappedHBTable<R, T> hbTable = validateHBClass(clazz);
        return hbTable.getFamiliesAndVersions();
    }


    /**
     * Checks whether input class can be converted to HBase data types and vice-versa
     *
     * @param clazz {@link Class} you intend to validate (must implement {@link HBRecord} interface)
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return <code>true</code> or <code>false</code>
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> boolean isValid(Class<T> clazz) {
        try {
            validateHBClass(clazz);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * For your bean-like {@link Class}, get all fields mapped to HBase columns
     *
     * @param clazz Bean-like {@link Class} (must implement {@link HBRecord} interface) whose fields you intend to read
     * @param <R>   Data type of row key
     * @param <T>   Entity type
     * @return A {@link Map} with keys as field names and values as instances of {@link Field}
     */
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Field> getHBColumnFields(Class<T> clazz) {
        validateHBClass(clazz);
        return getHBColumnFields0(clazz);
    }

    /**
     * 获取列族和唯一标识
     * @param clazz 类字节码
     * @param <R> rowKey
     * @param <T> 返回值
     * @return key-列唯一标识，value-属性
     */
    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Field> getHBColumnFields0(Class<T> clazz) {
        Map<String, Field> mappings = new LinkedHashMap<>();
        Class<?> thisClass = clazz;
        //递归获取所有的属性（包括父类）
        while (thisClass != null && thisClass != Object.class) {
            for (Field field : thisClass.getDeclaredFields()) {
                //版本信息是否存在
                if (new WrappedHBColumn(field).isPresent()) {
                    mappings.put(field.getName(), field);
                }
            }
            Class<?> parentClass = thisClass.getSuperclass();
            thisClass = parentClass.isAnnotationPresent(MappedSuperClass.class) ? parentClass : null;
        }
        return mappings;
    }
}