package veil.hdp.hive.jdbc.metadata;


import org.slf4j.Logger;
import veil.hdp.hive.jdbc.Builder;

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

        private HiveType hiveType;

        private Integer scale;
        private Integer precision;
        private Integer maxLength;

        private ColumnTypeDescriptorBuilder() {
        }


        public ColumnTypeDescriptorBuilder hiveType(HiveType hiveType) {
            this.hiveType = hiveType;
            return this;
        }

        public ColumnTypeDescriptorBuilder scale(Integer scale) {
            this.scale = scale;
            return this;
        }

        public ColumnTypeDescriptorBuilder precision(Integer precision) {
            this.precision = precision;
            return this;
        }

        public ColumnTypeDescriptorBuilder maxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public ColumnTypeDescriptor build() {

            return new ColumnTypeDescriptor(hiveType, scale, precision, maxLength);


        }


    }
}
