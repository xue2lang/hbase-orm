package com.flipkart.hbaseobjectmapper;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;

/**
 * Maps an entity field to an HBase column
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBColumn {

    /**
     * Name of HBase column family
     */
    String family();

    /**
     * Name of HBase column
     */
    String column();

    /**
     * Optional custom flags that will be passed to your {@link com.flipkart.hbaseobjectmapper.codec.Codec#serialize(Serializable) serialize} and {@link com.flipkart.hbaseobjectmapper.codec.Codec#deserialize(byte[], Type) deserialize} methods as a <code>Map&lt;String, String&gt;</code>
     */
    String[] flags() default {};
}
