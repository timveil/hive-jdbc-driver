package veil.hdp.hive.jdbc;

public enum AuthenticationMode {
    // SEE https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2
    // hive.server2.authentication – Authentication mode, default NONE. Options are NONE (uses plain SASL), NOSASL, KERBEROS, LDAP, PAM and CUSTOM.

    NONE,NOSASL,LDAP,KERBEROS,PAM
}
