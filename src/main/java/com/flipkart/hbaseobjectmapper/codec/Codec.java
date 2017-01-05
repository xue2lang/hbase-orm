package com.flipkart.hbaseobjectmapper.codec;


import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Interface that defines serialization and deserialization behavior for {@link com.flipkart.hbaseobjectmapper.HBObjectMapper HBObjectMapper}
 */
public interface Codec {
    /**
     * Serialize object to a <code>byte[]</code>
     *
     * @param object Object to be serialized
     * @return byte array - this would be used 'as is' in setting the column value in HBase row
     * @throws SerializationException If serialization fails (e.g. when a class uses a type that isn't serializable by this codec)
     */
    byte[] serialize(Serializable object, Map<String, String> flags) throws SerializationException;

    /**
     * Deserialize <code>byte[]</code> into object
     *
     * @param bytes byte array that represents serialized object
     * @param type  Java type to which this <code>byte[]</code> needs to be deserialized to
     * @return object
     * @throws DeserializationException If deserialization fails (e.g. malformed string or definition of a type used isn't available at runtime)
     */
    Serializable deserialize(byte[] bytes, Type type, Map<String, String> flags) throws DeserializationException;

    /**
     * Check whether a specific type can be deserialized using this codec
     *
     * @param type Java type
     * @return <code>true</code> or <code>false</code>
     */
    boolean canDeserialize(Type type);

}
