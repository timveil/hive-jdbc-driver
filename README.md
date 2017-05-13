# hive-jdbc


hive-jdbc-1.2.1000.2.5.3.0-37-standalone.jar is 20,103,967 bytes

hive-jdbc-1.2.1000.2.6.0.3-8-standalone.jar is 19,955,690 bytes

hive versions in 2.6:

hive 1:
1.2.1000.2.6.0.3-8

hive 2:
2.1.0.2.6.0.3-8

```
HIVE_CLI_SERVICE_PROTOCOL_V1,

// V2 adds support for asynchronous execution
HIVE_CLI_SERVICE_PROTOCOL_V2

// V3 add varchar type, primitive type qualifiers
HIVE_CLI_SERVICE_PROTOCOL_V3

// V4 add decimal precision/scale, char type
HIVE_CLI_SERVICE_PROTOCOL_V4

// V5 adds error details when GetOperationStatus returns in error state
HIVE_CLI_SERVICE_PROTOCOL_V5

// V6 uses binary type for binary payload (was string) and uses columnar result set
HIVE_CLI_SERVICE_PROTOCOL_V6

// V7 adds support for delegation token based connection
HIVE_CLI_SERVICE_PROTOCOL_V7

// V8 adds support for interval types
HIVE_CLI_SERVICE_PROTOCOL_V8

// V9 adds support for serializing ResultSets in SerDe
HIVE_CLI_SERVICE_PROTOCOL_V9

// V10 adds support for in place updates via GetOperationStatus
HIVE_CLI_SERVICE_PROTOCOL_V10
```

```
 Options are NONE (uses plain SASL), NOSASL, KERBEROS, LDAP, PAM and CUSTOM.
Set following for KERBEROS mode:
```


https://community.hortonworks.com/articles/28537/user-authentication-from-windows-workstation-to-hd.html

http://web.mit.edu/kerberos/dist/index.html

C:\Program Files\MIT\Kerberos

Copy the krb5.conf file (from the HDP KDC) to above mentioned location and rename krb5.conf to krb5.ini.


had to specify -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini when starting up dbviz, setting environment variable, KRB5_CONFIG, did not work.

must specify Djavax.security.auth.useSubjectCredsOnly=false for things to work. need to investigate why