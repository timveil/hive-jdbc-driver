package veil.hdp.hive.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public class HiveDriverPropertyInfo {

    private final String name;
    private final String description;
    private final String defaultValue;
    private final boolean required;
    private final String[] choices;

    public HiveDriverPropertyInfo(String name, String description, String defaultValue, boolean required, String[] choices) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.required = required;
        this.choices = choices;
    }

    public DriverPropertyInfo build(Properties properties) {
        String propValue = name.equals(HiveDriverStringProperty.PASSWORD.toString()) ? "" : properties.getProperty(name);

        if (null == propValue)
            propValue = defaultValue;

        DriverPropertyInfo info = new DriverPropertyInfo(name, propValue);
        info.description = description;
        info.required = required;
        info.choices = choices;

        return info;
    }

    public String getName() {
        return name;
    }
}
