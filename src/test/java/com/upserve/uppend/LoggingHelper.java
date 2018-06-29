package com.upserve.uppend;

import com.upserve.uppend.lookup.LookupMetadata;
import org.slf4j.*;

import java.lang.reflect.*;

public class LoggingHelper {
    public static void resetLogger(Class clazz, String fieldName) throws Exception {
        setLogger(clazz, fieldName, LoggerFactory.getLogger(clazz));
    }

    public static void setLogger(Class clazz, String fieldName, Logger log) throws Exception {
        Field field = LookupMetadata.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, log);
    }
}
