package veil.hdp.hive.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum HiveDriverProperty {
    HOST_NAME("host", null, true, ""),
    DATABASE_NAME("database", "default", true, ""),
    USER("user", null, true, ""),
    PASSWORD("password", null, false, ""),
    PORT_NUMBER("port", "10000", true, ""),

    TRANSPORT_MODE("transportMode", TransportMode.binary.toString(), false, "", TransportMode.binary.toString(), TransportMode.http.toString()),

    ZOOKEEPER_DISCOVERY_ENABLED("zkEnabled", Boolean.FALSE.toString(), false, ""),
    ZOOKEEPER_DISCOVERY_NAMESPACE("zkNamespace", "hiveserver2", false, ""),
    ZOOKEEPER_DISCOVERY_RETRY("zkRetry", "1000", false, "");;

    private String name;
    private String defaultValue;
    private boolean required;
    private String description;
    private String[] choices;


    HiveDriverProperty(String name, String defaultValue, boolean required, String description) {
        this(name, defaultValue, required, description, (String[]) null);
    }

    HiveDriverProperty(String name, String defaultValue, boolean required, String description, String... choices) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.required = required;
        this.description = description;
        this.choices = choices;
    }

    public String getName() {
        return name;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    public String getDescription() {
        return description;
    }

    public String[] getChoices() {
        return choices;
    }

    public void setDefaultValue(Properties properties) {
        if (defaultValue != null) {
            properties.setProperty(name, defaultValue);
        }
    }

    public void set(Properties properties, String value) {
        if (value == null) {
            properties.remove(name);
        } else {
            properties.setProperty(name, value);
        }
    }

    public void set(Properties properties, boolean value) {
        properties.setProperty(name, Boolean.toString(value));
    }

    public void set(Properties properties, int value) {
        properties.setProperty(name, Integer.toString(value));
    }

    public String get(Properties properties) {
        return properties.getProperty(name, defaultValue);
    }

    public boolean getBoolean(Properties properties) {
        return Boolean.valueOf(get(properties));
    }

    public Integer getInteger(Properties properties) {
        String value = get(properties);

        if (value == null) {
            return null;
        }

        return Integer.parseInt(value);
    }

    public int getInt(Properties properties) {
        String value = get(properties);

        return Integer.parseInt(value);
    }

    public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, get(properties));
        propertyInfo.required = required;
        propertyInfo.description = description;
        propertyInfo.choices = choices;

        return propertyInfo;
    }
}
