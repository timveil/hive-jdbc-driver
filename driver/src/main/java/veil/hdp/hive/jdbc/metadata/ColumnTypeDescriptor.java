package veil.hdp.hive.jdbc.metadata;


import org.slf4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.bindings.*;

import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class ColumnTypeDescriptor {

    private static final Logger log = getLogger(ColumnTypeDescriptor.class);

    private final HiveType hiveType;

    private final Integer scale;
    private final Integer precision;
    private final Integer characterMaximumLength;

    private ColumnTypeDescriptor(HiveType hiveType, Integer scale, Integer precision, Integer characterMaximumLength) {
        this.hiveType = hiveType;
        this.scale = scale;
        this.precision = precision;
        this.characterMaximumLength = characterMaximumLength;
    }

    public static ColumnTypeDescriptorBuilder builder() {
        return new ColumnTypeDescriptorBuilder();
    }

    public HiveType getHiveType() {
        return hiveType;
    }

    public Integer getScale() {
        return scale;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getCharacterMaximumLength() {
        return characterMaximumLength;
    }

    @Override
    public String toString() {
        return "ColumnTypeDescriptor{" +
                "hiveType=" + hiveType +
                ", scale=" + scale +
                ", precision=" + precision +
                ", characterMaximumLength=" + characterMaximumLength +
                '}';
    }

    public static class ColumnTypeDescriptorBuilder implements Builder<ColumnTypeDescriptor> {

        private TTypeDesc typeDesc;
        private HiveType hiveType;

        private ColumnTypeDescriptorBuilder() {
        }

        ColumnTypeDescriptorBuilder thriftType(TTypeDesc typeDesc) {
            this.typeDesc = typeDesc;
            return this;
        }

        public ColumnTypeDescriptorBuilder hiveType(HiveType hiveType) {
            this.hiveType = hiveType;
            return this;
        }

        public ColumnTypeDescriptor build() {

            if (hiveType != null) {
                return new ColumnTypeDescriptor(hiveType, null, null, null);
            } else {

                // don't see any instances where more than one value is returned from thrift eventhough the IDL says it should
                TTypeEntry entry = typeDesc.getTypes().get(0);

                HiveType hiveType = null;

                Integer scale = null;
                Integer precision = null;
                Integer maxLength = null;

                if (entry.isSetPrimitiveEntry()) {
                    TPrimitiveTypeEntry primitiveEntry = entry.getPrimitiveEntry();

                    log.debug("primitive entry {}", primitiveEntry);

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
                    throw new HiveException("unable to determine type for entry [" + entry + "]");
                }

                log.debug("new way hive type is {}", hiveType);

                return new ColumnTypeDescriptor(hiveType, scale, precision, maxLength);
            }

        }

        private Integer getQualifier(String key, Map<String, TTypeQualifierValue> map) {
            if (map.containsKey(key)) {
                TTypeQualifierValue qualifierValue = map.get(key);

                if (qualifierValue.isSetI32Value()) {
                    return qualifierValue.getI32Value();
                }
            }

            return null;
        }
    }
}
