package veil.hdp.hive.jdbc.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.metadata.ColumnTypeDescriptor;
import veil.hdp.hive.jdbc.metadata.HiveType;

import java.util.Map;

public class TypeDescriptorUtils {

    private static final Logger log =  LogManager.getLogger(TypeDescriptorUtils.class);

    public static ColumnTypeDescriptor getDescriptor(TTypeDesc typeDesc) {

        TTypeEntry entry = typeDesc.getTypes().get(0);

        HiveType hiveType = null;

        Integer scale = null;
        Integer precision = null;
        Integer maxLength = null;

        if (entry.isSetPrimitiveEntry()) {
            TPrimitiveTypeEntry primitiveEntry = entry.getPrimitiveEntry();

            if (primitiveEntry.isSetTypeQualifiers()) {
                TTypeQualifiers typeQualifiers = primitiveEntry.getTypeQualifiers();
                Map<String, TTypeQualifierValue> map = typeQualifiers.getQualifiers();

                scale = getQualifier("scale", map);
                precision = getQualifier("precision", map);
                maxLength = getQualifier("characterMaximumLength", map);
            }

            if (primitiveEntry.isSetType()) {
                hiveType = HiveType.valueOf(primitiveEntry.getType());
            }
        }

        if (hiveType == null) {
            throw new HiveException("unable to determine type for entry [" + entry + ']');
        }

        return ColumnTypeDescriptor.builder().hiveType(hiveType).scale(scale).precision(precision).maxLength(maxLength).build();
    }

    private static Integer getQualifier(String key, Map<String, TTypeQualifierValue> map) {
        if (map.containsKey(key)) {
            TTypeQualifierValue qualifierValue = map.get(key);

            if (qualifierValue.isSetI32Value()) {
                return qualifierValue.getI32Value();
            }
        }

        return null;
    }
}
