package com.flipkart.hbaseobjectmapper.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.flipkart.hbaseobjectmapper.codec.Codec;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * A flag for {@link Codec Codec} (specify parameter name and value)
 * <p>
 * This is to be used exclusively for input to {@link HBColumn#codecFlags() codecFlags} parameter of {@link HBColumn} and {@link HBColumnMultiVersion} annotations
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface Flag {

    /**
     * 特殊入参的key
     * @return
     */
    String name();

    /**
     * 特殊入参的value
     * @return
     */
    String value();
}
