package veil.hdp.hive.jdbc;

public enum HiveDriverIntProperty {

    PORT_NUMBER("port", "", 10000),

    //the current query timeout limit in seconds; zero means there is no limit
    STATEMENT_QUERY_TIMEOUT("statement.query.timeout", "", 0),

    //zero means there is no limit
    STATEMENT_MAX_ROWS("statement.max.rows", "", 0),

    //If the value specified is zero, then the hint is ignored. should be default for resultset
    STATEMENT_FETCH_SIZE("statement.fetch.size", "", 1000),

    ZOOKEEPER_DISCOVERY_RETRY("zookeeper.discovery.retry", "", 1000);

    private String name;
    private int defaultValue;
    private String description;

    HiveDriverIntProperty(String name, String description, int defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public int getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }
}
