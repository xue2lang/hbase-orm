package com.flipkart.hbaseobjectmapper.codec;


import java.lang.reflect.Type;

public interface Codec {
    byte[] serialize(Object object) throws SerializationException;

    Object deserialize(byte[] bytes, Type type) throws DeserializationException;

    boolean canDeserialize(Type type);

}
