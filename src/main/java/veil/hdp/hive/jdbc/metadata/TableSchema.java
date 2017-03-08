package veil.hdp.hive.jdbc.metadata;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.thrift.TTableSchema;

import java.util.List;

public class TableSchema extends org.apache.hive.service.cli.TableSchema {

    public TableSchema(TTableSchema tTableSchema) {
        super(tTableSchema);
    }

    public ColumnDescriptor getColumnDescriptorForName(String columnName) {


        for (ColumnDescriptor columnDescriptor : getColumnDescriptors()) {

            String originalName = columnDescriptor.getName();

            List<String> nameParts = Splitter.on(".").omitEmptyStrings().trimResults().splitToList(originalName);

            String normalizedName = Lists.reverse(nameParts).get(0);

            if (normalizedName.equalsIgnoreCase(columnName)) {
                return columnDescriptor;
            }
        }

        return null;
    }

}
