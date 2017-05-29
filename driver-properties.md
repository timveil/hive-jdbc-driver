# Driver Properties

HS2 = Hiverserver2

if property has a corresponding hive configuration property, it will likely be overwritten when zookeeper discovery is enabled

## Common Properties

### Host

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| host |  | true | [hive.server2.thrift.bind.host](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The fully qualified domain name (FQDN) of the HS2 instance or, if Zookeeper discovery is enabled, the name of the Zookeeper host.  When Zookeeper discovery is enabled the value of `hive.server2.thrift.bind.host` will be returned by Zookeeper and used by the driver as the `host` value.

### Database Name

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| database | default | true | none  |

The database name used by the driver to establish a connection.

### User

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| user |  | false | none |

When "Authentication Mode" equals "NONE" this property is ignored.  If "Authentication Mode" equals "KERBEROS" and "Kerberos Mode" equals "PASSWORD", this property can be the local Kerberos "principal"

### Password

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| password |  | false | none |

When "Authentication Mode" equals "NONE" this property is ignored.  If "Authentication Mode" equals "KERBEROS" and "Kerberos Mode" equals "PASSWORD", this property can be the password for the local Kerberos "principal"

### Port Number

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| port | 1000 | true | [hive.server2.thrift.port](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The port used to by the driver to establish a connection.  If Zookeeper discovery is enabled, this is the Zookeeper client port (e.g. 2181), otherwise it should be the value specified by `hive.server2.thrift.port` in Hive's configuration properties.  When Zookeeper discovery is enabled the value of `hive.server2.thrift.port` will be returned by Zookeeper and used by the driver as the `port` value.

### Transport Mode

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| transportMode | binary | false | [hive.server2.transport.mode](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

Transport Mode specifies the Thrift transport interface used to communicate with the HS2 instance.  This driver supports both `binary` and `http` modes.  This value is specified in Hive with the property `hive.server2.transport.mode`.  Because this driver bundles its `http` and `binary` versions separately, it is not necessary to use the property.  The appropriate value is set depending on the driver version used.

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

## Binary Properties

### Thrift Protocol Version

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| thriftVersion | 9 or `TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10` | false | none |

The Thrift Protocol Version helps to define the features/functions available as defined in the Thrift IDL or `.thrift` file.  This allows interoperability between Hive versions.

## HTTP Properties

### SSL Enbled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpsEnabled | false | false | [hive.server2.use.ssl](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

Instructs the driver to use SSL when communicating over HTTP.

### HTTP Endpoint

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpEndpoint | cliservice | false | [hive.server2.thrift.http.path](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The path portion of the Thrift HTTP endpoint URL.

## Zookeeper Properties

### Zookeeper Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| zkEnabled | false | false | none |

Instructs the driver to lookup Hive configuration in Zookeeper.  Values returned from this lookup are then used to configure the driver connection.  The following Hive configuration properties may be returned by Zookeeper:
*  hive.server2.authentication
*  hive.server2.transport.mode
*  hive.server2.thrift.sasl.qop
*  hive.server2.thrift.bind.host
*  hive.server2.thrift.port
*  hive.server2.use.SSL

### Zookeeper Discovery Namespace

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| zkNamespace | hiveserver2 | false | [hive.server2.zookeeper.namespace](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The root path or namespace in Zookeeper under which Hive configuration data is stored.  If LLAP is enabled this value may be `hiveserver2-hive2`.

### Zookeeper Retry Wait Time

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| zkRetry | 1000 | false | none |

The amount of time, in milliseconds, that the Zookeeper client will wait before attempting a single retry.