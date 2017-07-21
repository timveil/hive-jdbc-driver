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
| database | `default` | true | none  |

The database name used by the driver to establish a connection.

### User

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| user |  | false | none |

When `authMode` equals `NONE` this property is ignored.  If `authMode` equals `KERBEROS` and `krb5Mode` equals `PASSWORD`, this property can be the local Kerberos "principal"

### Password

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| password |  | false | none |

When `authMode` equals `NONE` this property is ignored.  If `authMode` equals `KERBEROS` and `krb5Mode` equals `PASSWORD`, this property can be the password for the local Kerberos "principal"

### Port Number

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| port | `10000` | true | [hive.server2.thrift.port](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The port used to by the driver to establish a connection.  If Zookeeper discovery is enabled, this is the Zookeeper client port (e.g. 2181), otherwise it should be the value specified by `hive.server2.thrift.port` in Hive's configuration properties.  When Zookeeper discovery is enabled the value of `hive.server2.thrift.port` will be returned by Zookeeper and used by the driver as the `port` value.

### Transport Mode

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| transportMode | `binary` | false | [hive.server2.transport.mode](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

Transport Mode specifies the Thrift transport interface used to communicate with the HS2 instance.  This driver supports both `binary` and `http` modes.  This value is specified in Hive with the property `hive.server2.transport.mode`.  Because this driver bundles its `http` and `binary` versions separately, it is not necessary to use the property.  The appropriate value is set depending on the driver version used.

### Authentication Mode

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| authMode | `NONE` | false | [hive.server2.authentication](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The Authentication Mode of the HS2 instance as specified by the Hive configuration property `hive.server2.authentication`.  The allowable values of this property are:
* `NONE` - No user or password are required.  In the case of a `binary` connection, the Thrift socket uses plain SASL.
* `NOSASL` - In the case of a `binary` connection, a raw Thrift socket is used.  Not applicable to `http` connections.
* `KERBEROS` - A valid Kerberos principal is required to establish a connection to HS2
* `LDAP` - Not currently supported
* `PAM` - Not currently supported

### Thrift Protocol Version

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| thriftVersion | `7` | false | none |

The Thrift Protocol Version helps to define the features/functions available as defined in the Thrift IDL or `.thrift` file.  This allows interoperability between Hive versions.

*   0 = `HIVE_CLI_SERVICE_PROTOCOL_V1`, initial version
*   1 = `HIVE_CLI_SERVICE_PROTOCOL_V2`, added support for asynchronous execution (added with [HIVE-4617](https://issues.apache.org/jira/browse/HIVE-4617), Hive 0.12.0)
*   2 = `HIVE_CLI_SERVICE_PROTOCOL_V3`, added varchar type, primitive type qualifiers (added with [HIVE-5209](https://issues.apache.org/jira/browse/HIVE-5209), Hive 0.12.0)
*   3 = `HIVE_CLI_SERVICE_PROTOCOL_V4`, added decimal precision/scale, char type (added with [HIVE-5780](https://issues.apache.org/jira/browse/HIVE-5780), Hive 0.13.0)
*   4 = `HIVE_CLI_SERVICE_PROTOCOL_V5`, added error details when GetOperationStatus returns in error state (added with [HIVE-5230](https://issues.apache.org/jira/browse/HIVE-5230), Hive 0.13.0)
*   5 = `HIVE_CLI_SERVICE_PROTOCOL_V6`, uses binary type for binary payload (was string) and uses columnar result set (added with [HIVE-3746](https://issues.apache.org/jira/browse/HIVE-3746), Hive 0.13.0)
*   6 = `HIVE_CLI_SERVICE_PROTOCOL_V7`, added support for delegation token based connection (added with [HIVE-6647](https://issues.apache.org/jira/browse/HIVE-6647), Hive 0.13.0)
*   7 = `HIVE_CLI_SERVICE_PROTOCOL_V8`, added support for interval types (added with [HIVE-10037](https://issues.apache.org/jira/browse/HIVE-10037), Hive 1.2.0)
*   8 = `HIVE_CLI_SERVICE_PROTOCOL_V9`, added support for serializing ResultSets in SerDe (added with [HIVE-14191](https://issues.apache.org/jira/browse/HIVE-14191), Hive 2.1.1, 7/16)
*   9 = `HIVE_CLI_SERVICE_PROTOCOL_V10`, added support for in place updates via GetOperationStatus (added with [HIVE-15473](https://issues.apache.org/jira/browse/HIVE-15473), Hive 2.2.0, 2/17)

## Binary Properties

### Thrift Socket Timeout

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| thriftSocketTimeout | `0` | false | none |

The socket read timeout, in milliseconds, of Thrift's TSocket.  Default of `0` means there is no timeout.

### Thrift Connection Timeout

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| thriftConnectionTimeout | `0` | false | none |

The connection timeout, in milliseconds, of Thrift's TSocket.  Default of `0` means there is no timeout.

## SSL Properties

### SSL Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslEnabled | `false` | false | [hive.server2.use.ssl](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

Instructs the driver to use SSL.  Applies to both `http` and `binary` transport mode.

### TrustStore Path

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslTrustStorePath |  | false | none |

Location of the TrustStore on disk.

### TrustStore Type

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslTrustStoreType | `JKS` | false | none |

Used to generate an instance of a [KeyStore](https://docs.oracle.com/javase/8/docs/api/java/security/KeyStore.html).  Possible values can be found [here](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#KeyStore).

### TrustStore Password

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslTrustStorePassword |  | false | none |

### Two-way SSL Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslTwoWayEnabled | `false` | false | none |

todo: need very good explanation of this.

Only applicable when `transportMode` equals `http`

### KeyStore Path

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslKeyStorePath |  | false | none |

Location of the KeyStore on disk.

### KeyStore Type

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslKeyStoreType | `JKS` | false | none |

Used to generate an instance of a [KeyStore](https://docs.oracle.com/javase/8/docs/api/java/security/KeyStore.html).  Possible values can be found [here](https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#KeyStore).

### KeyStore Password

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| sslKeyStorePassword |  | false | none |

## HTTP Properties

### HTTP Endpoint

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpEndpoint | `cliservice` | false | [hive.server2.thrift.http.path](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The path portion of the Thrift HTTP endpoint URL.

### Connection Pool Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpPoolEnabled | `true` | false | none |

Instructs the driver to use the [PoolingHttpClientConnectionManager](https://hc.apache.org/httpcomponents-client-ga/httpclient/apidocs/org/apache/http/impl/conn/PoolingHttpClientConnectionManager.html) instead of [BasicHttpClientConnectionManager](https://hc.apache.org/httpcomponents-client-ga/httpclient/apidocs/org/apache/http/impl/conn/BasicHttpClientConnectionManager.html) when building the [CloseableHttpClient](https://hc.apache.org/httpcomponents-client-ga/httpclient/apidocs/org/apache/http/impl/client/CloseableHttpClient.html) for use with Thrift.

### Max Pooled Connections

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpPoolMax | `100` | false | none |

The maximum number of connections in the pool when `httpPoolEnabled` is `true`

### Max Pooled Connections per Route

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpPoolMaxRoute | `20` | false | none |

The maximum number of connections in the pool per route when `httpPoolEnabled` is `true`

### Cookie Replay Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpCookieReplayEnabled | `true` | false | none |

Enables cookie replay as described in https://issues.apache.org/jira/browse/HIVE-9709.  This prevents re-authentication on subsequent requests.

### Cookie Name

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| httpCookieName | `hive.server2.auth` | false | none |

When `httpCookieReplayEnabled` is `true`, this is the name of the cookie that must be present to prevent re-authentication.

## Zookeeper Properties

### Zookeeper Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| zkEnabled | `false` | false | none |

Instructs the driver to lookup Hive configuration in Zookeeper.  Values returned from this lookup are then used to configure the driver connection.  The following Hive configuration properties may be returned by Zookeeper:
*  `hive.server2.authentication`
*  `hive.server2.transport.mode`
*  `hive.server2.thrift.sasl.qop`
*  `hive.server2.thrift.bind.host`
*  `hive.server2.thrift.port`
*  `hive.server2.use.SSL`

### Discovery Namespace

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| zkNamespace | `hiveserver2` | false | [hive.server2.zookeeper.namespace](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

The root path or namespace in Zookeeper under which Hive configuration data is stored.  If LLAP is enabled this value may be `hiveserver2-hive2`.

### Retry Wait Time

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| zkRetry | `1000` | false | none |

The amount of time, in milliseconds, that the Zookeeper client will wait before attempting a single retry.

## Kerberos Properties

The following properties only apply when `authMode` equals `KERBEROS`

### Client Kerberos Mode

This defines how the driver authenticates on the client side.  This is not server side authentication.  In order to establish a jdbc connection using kerberos the client must be authenticated in one of the following ways so that a ticket can be shared with the server

todo: these values need very thorough explanations and examples

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| krb5Mode | `PASSWORD` | false | none |

* `PASSWORD` - must specify value for `user` and `password` properties
* `PREAUTH` - assumes a valid Subject has already been authenticated.  This is typically done in external application code.  See example: todo
* `KEYTAB` - must specify value for `user` and `krb5UserKeytab` properties

### Server Principal

todo: should this be named server or service?  also, not clear that the hive config property and the intent of this property are one in the same

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| krb5ServerPrincipal |  | true<sup>*</sup> | [hive.server2.authentication.kerberos.principal](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

This is the Kerberos principal that is valid on the HS2 server.

<sup>*</sup> required when `authMode` equals `KERBEROS`

### User Keytab

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| krb5UserKeytab |  | false | none |

Path to valid keytab

### Debug Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| krb5Debug | `true` | false | none |

Sets value for `sun.security.krb5.debug` as a System property

### Use Subject Credentials Only

todo: need to better understand what the heck this means

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| krb5SubjectOnly | `false` | false | none |

Sets value for `javax.security.auth.useSubjectCredsOnly` as a System Property.  See [here](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/BasicClientServer.html#useSub) for more details.

### SASL Quality of Protection

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| saslQOP | `auth` | false | [hive.server2.thrift.sasl.qop](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-HiveServer2) |

### SASL Server Authentication Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| saslAuth | `true` | false | none |

### JAAS Debug Enabled

| Property | Default Value | Required | Hive Configuration Property |
| :--- | :--- | :--- | :--- |
| jaasDebug | `true` | false | none |


## Apache Driver Property Mapping

This table outlines how the existing Apache Hive drivers properties map to this driver. You can use this to convert existing url's to a format consistent with this driver.  Many of the properties are similar but have been renamed slightly to improve consistency and clarity.

| Apache Driver Property | This Driver Property | Notes |
| :--- | :--- | :--- |
|`transportMode`|`transportMode`|Drivers are separated based on transport mode.  To use `binary` or `http` transport mode use the appropriate driver|
|`hive.server2.transport.mode`|`transportMode`|deprecated in Apache; `transportMode` is recommended instead|
|`httpPath`|`httpEndpoint`| |
|`hive.server2.thrift.http.path`|`httpEndpoint`|deprecated in Apache; `httpPath` is recommended instead|
|`ssl`|`sslEnabled`| |
|`twoWay`|`sslTwoWayEnabled`| |
|`sslTrustStore`|`sslTrustStorePath`| |
|`trustStorePassword`|`sslTrustStorePassword`| |
|`sslKeyStore`|`sslKeyStorePath`| |
|`keyStorePassword`|`sslKeyStorePassword`| |
|`serviceDiscoveryMode`|`zkEnabled`| |
|`zooKeeperNamespace`|`zkNamespace`| |
|`principal`|`krb5ServerPrincipal`| |
|`saslQop`|`saslQOP`| |
|`sasl.qop`|`saslQOP`|deprecated in Apache; `saslQop` is recommended instead|
|`cookieAuth`|`httpCookieReplayEnabled`| |
|`cookieName`|`httpCookieName`| |
|`kerberosAuthType`|`krb5Mode`| |

