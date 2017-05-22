# Driver Properties

HS2 = Hiverserver2

## Common

### Host

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| host |  | true | [hive.server2.thrift.bind.host](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The fully qualified domain name (FQDN) of the HS2 instance or, if Zookeeper discovery is enabled, the name of the Zookeeper host.  When Zookeeper discovery is enabled the value of `hive.server2.thrift.bind.host` will be returned by Zookeeper and used by the driver as the host value. 

### Database Name

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| database | default | true |   |

The database name used by the driver to establish a connection.

### User

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| user |  | false |   |

When "Authentication Mode" equals "NONE" this property is ignored.  If "Authentication Mode" equals "KERBEROS" and "Kerberos Mode" equals "PASSWORD", this property can be the local Kerberos "principal"

### Password

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| password |  | false |   |

When "Authentication Mode" equals "NONE" this property is ignored.  If "Authentication Mode" equals "KERBEROS" and "Kerberos Mode" equals "PASSWORD", this property can be the password for the local Kerberos "principal"

### Port Number

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| port | 1000 | true | [hive.server2.thrift.port](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The port used to by the driver to establish a connection.  If Zookeeper discovery is enabled, this is the Zookeeper client port (e.g. 2181), otherwise it should be the value specified by `hive.server2.thrift.port` in Hive's configuration properties.  When Zookeeper discovery is enabled the value of `hive.server2.thrift.port` will be returned by Zookeeper and used by the driver as the port value.

### Transport Mode

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| transportMode | binary | false | [hive.server2.transport.mode](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

Transport Mode specifies the Thrift transport interface used to communicate with the HS2 instance.  This driver supports both `binary` and `http` modes.  This value is specified in Hive with the property `hive.server2.transport.mode`.  Because this driver bundles its HTTP and Binary versions separately, it is not necessary to use the property.  The appropriate value is set depending on the driver version used.

### Authentication Mode

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| authMode | NONE | false | [hive.server2.authentication](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The Authentication Mode of the HS2 instance as specified by the Hive configuration property `hive.server2.authentication`.  The allowable values of this property are:
* NONE - in the case of a `binary` connection, the Thrift socket uses plain SASL.  No user or password are required.  todo: need HTTP
* NOSASL - in the case of a `binary` connection, a raw Thrift socket is used. todo: need HTTP and better explanation
* KERBEROS - valid Kerberos principal is required to establish a connection to HS2
* LDAP - Not currently supported
* PAM - Not currently supported
