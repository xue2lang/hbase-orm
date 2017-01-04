package com.flipkart.hbaseobjectmapper.codec;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.hbaseobjectmapper.exceptions.BadHBaseLibStateException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * A codec that:<ul>
 * <li>Uses HBase's native methods to serialize/deserialize objects of types {@link Boolean}, {@link Short}, {@link Integer}, {@link Long}, {@link Float}, {@link Double}, {@link String} and {@link BigDecimal}</li>
 * <li>Uses Jackson's serializer/deserializer for all other types (JSON)</li>
 * <li>Serializes <code>null</code> as <code>null</code></li>
 * </ul>
 */

public class JacksonJsonCodec implements Codec {
    private static final Map<Class, String> fromBytesMethodNames = new HashMap<Class, String>() {
        {
            put(Boolean.class, "toBoolean");
            put(Short.class, "toShort");
            put(Integer.class, "toInt");
            put(Long.class, "toLong");
            put(Float.class, "toFloat");
            put(Double.class, "toDouble");
            put(String.class, "toString");
            put(BigDecimal.class, "toBigDecimal");
        }
    };

    private static final Map<Class, Class> nativeCounterParts = new HashMap<Class, Class>() {
        {
            put(Boolean.class, boolean.class);
            put(Short.class, short.class);
            put(Long.class, long.class);
            put(Integer.class, int.class);
            put(Float.class, float.class);
            put(Double.class, double.class);
        }
    };

    private static final Map<Class, Method> fromBytesMethods, toBytesMethods;
    private static final Map<Class, Constructor> constructors;

    static {
        try {
            fromBytesMethods = new HashMap<>(fromBytesMethodNames.size());
            toBytesMethods = new HashMap<>(fromBytesMethodNames.size());
            constructors = new HashMap<>(fromBytesMethodNames.size());
            Method fromBytesMethod, toBytesMethod;
            Constructor<?> constructor;
            for (Map.Entry<Class, String> e : fromBytesMethodNames.entrySet()) {
                Class<?> clazz = e.getKey();
                String toDataTypeMethodName = e.getValue();
                fromBytesMethod = Bytes.class.getDeclaredMethod(toDataTypeMethodName, byte[].class);
                toBytesMethod = Bytes.class.getDeclaredMethod("toBytes", nativeCounterParts.containsKey(clazz) ? nativeCounterParts.get(clazz) : clazz);
                constructor = clazz.getConstructor(String.class);
                fromBytesMethods.put(clazz, fromBytesMethod);
                toBytesMethods.put(clazz, toBytesMethod);
                constructors.put(clazz, constructor);
            }
        } catch (Exception ex) {
            throw new BadHBaseLibStateException(ex);
        }
    }


    private final ObjectMapper objectMapper;
    private final boolean serializeAsString;

    /**
     * Construct an object of class {@link JacksonJsonCodec} with custom instance of Jackson's Object Mapper and specify whether all numeric fields of input object have to serialized as string before finally getting serialized to `byte[]`
     *
     * @param objectMapper      Instance of Jackson's Object Mapper
     * @param serializeAsString (Applicable to numeric fields) Store field value in it's string representation (e.g. (int)560034 is stored as "560034")
     */
    public JacksonJsonCodec(ObjectMapper objectMapper, boolean serializeAsString) {
        this.objectMapper = objectMapper;
        this.serializeAsString = serializeAsString;
    }

    /**
     * Construct an object of class {@link JacksonJsonCodec} with custom instance of Jackson's Object Mapper
     *
     * @param objectMapper Instance of Jackson's Object Mapper
     */
    public JacksonJsonCodec(ObjectMapper objectMapper) {
        this(objectMapper, false);
    }

    /**
     * Construct an object of class {@link JacksonJsonCodec} and specify whether all numeric fields of input object have to serialized as string before finally getting serialized to `byte[]`
     *
     * @param serializeAsString (Applicable to numeric fields) Store field value in it's string representation (e.g. (int)560034 is stored as "560034")
     */
    public JacksonJsonCodec(boolean serializeAsString) {
        this(new ObjectMapper(), serializeAsString);
    }

    /**
     * Construct an object of class {@link JacksonJsonCodec} with default settings
     */
    public JacksonJsonCodec() {
        this(false);
    }

    /*
    * @inherit
    */
    @Override
    public byte[] serialize(Serializable object, Map<String, String> flags) throws SerializationException {
        if (object == null)
            return null;
        Class clazz = object.getClass();
        if (toBytesMethods.containsKey(clazz)) {
            try {
                Method toBytesMethod = toBytesMethods.get(clazz);
                return serializeAsString ? Bytes.toBytes(String.valueOf(object)) : (byte[]) toBytesMethod.invoke(null, object);
            } catch (Exception e) {
                throw new SerializationException(String.format("Could not serialize value of type %s using HBase's native methods", clazz.getName()), e);
            }
        } else {
            try {
                return objectMapper.writeValueAsBytes(object);
            } catch (Exception e) {
                throw new SerializationException("Could not serialize object to JSON using Jackson", e);
            }
        }
    }

    /*
    * @inherit
    */
    @Override
    public Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException {
        if (bytes == null)
            return null;
        if (type instanceof Class && fromBytesMethods.containsKey(type)) {
            try {
                Serializable fieldValue;
                if (serializeAsString) {
                    Constructor constructor = constructors.get(type);
                    fieldValue = (Serializable) constructor.newInstance(Bytes.toString(bytes));
                } else {
                    Method method = fromBytesMethods.get(type);
                    fieldValue = (Serializable) method.invoke(null, new Object[]{bytes});
                }
                return fieldValue;
            } catch (Exception e) {
                throw new DeserializationException("Could not deserialize byte array into an object using HBase's native methods", e);
            }
        } else {
            try {
                return objectMapper.readValue(bytes, objectMapper.constructType(type));
            } catch (Exception e) {
                throw new DeserializationException("Could not deserialize JSON into an object using Jackson", e);
            }
        }

    }


    /*
    * @inherit
    */
    @Override
    public boolean canDeserialize(Type type) {
        JavaType javaType = objectMapper.constructType(type);
        return objectMapper.canDeserialize(javaType);
    }
}
