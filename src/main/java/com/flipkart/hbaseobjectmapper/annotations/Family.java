package com.flipkart.hbaseobjectmapper.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Represents a column family in HBase
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface Family {
    /**
     * Column family name
     *
     * @return Column family name
     */
    String name();

    /**
     * Maximum number of versions configured for a given column family of the HBase table
     *
     * @return Max number of versions
     */
    int versions() default 1;
}
