# thrift-client

this is not used any longer...

thrift --gen java:beans -out src/main/java src/main/resources/TCLIService.thrift

E:\dev\tools\thrift>

.\thrift-0.9.3.exe --gen java:beans -out out TCLIService.thrift


now thrift plugin is used.  Use maven profile to select which .thrift file to use

```
<thriftSourceRoot>${basedir}/src/main/thrift/${hive.version}</thriftSourceRoot>
```

where ${hive.version} is set by the maven profile
