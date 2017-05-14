package veil.hdp.hive.jdbc.security;

import java.security.Principal;

public class JdbcPrincipal implements Principal {

    private final String name;

    public JdbcPrincipal(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return name;
    }
}
