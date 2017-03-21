package veil.hdp.hive.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum HiveDriverProperty {
    HOST_NAME("host", null, true, null, "hive.server2.thrift.bind.host"),
    DATABASE_NAME("database", "default", true, null, null),
    USER("user", null, true, null, null),
    PASSWORD("password", null, false, null, null),
    PORT_NUMBER("port", "10000", true, null, "hive.server2.thrift.port"),

    TRANSPORT_MODE("transportMode", TransportMode.binary.toString(), false, null, "hive.server2.transport.mode", TransportMode.binary.toString(), TransportMode.http.toString()),

    ZOOKEEPER_DISCOVERY_ENABLED("zkEnabled", Boolean.FALSE.toString(), false, null, null),
    ZOOKEEPER_DISCOVERY_NAMESPACE("zkNamespace", "hiveserver2", false, null, null),
    ZOOKEEPER_DISCOVERY_RETRY("zkRetry", "1000", false, null, null);

    private String name;
    private String defaultValue;
    private boolean required;
    private String description;
    private String hiveConfName;
    private String[] choices;


    HiveDriverProperty(String name, String defaultValue, boolean required, String description, String hiveConfName) {
        this(name, defaultValue, required, description, hiveConfName, (String[]) null);
    }

    HiveDriverProperty(String name, String defaultValue, boolean required, String description, String hiveConfName, String... choices) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.required = required;
        this.description = description;
        this.hiveConfName = hiveConfName;
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

    public String getHiveConfName() {
        return hiveConfName;
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

    public static HiveDriverProperty forAlias(String alias) {
        for (HiveDriverProperty property : HiveDriverProperty.values()) {
            if (property.getHiveConfName() != null && property.getHiveConfName().equalsIgnoreCase(alias)) {
                return property;
            }
        }

        return null;
    }
}
