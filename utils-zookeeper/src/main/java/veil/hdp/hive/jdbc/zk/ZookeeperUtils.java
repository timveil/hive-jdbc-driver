package veil.hdp.hive.jdbc.zk;


import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.HiveDriverProperty;
import veil.hdp.hive.jdbc.core.HiveException;
import veil.hdp.hive.jdbc.core.utils.DriverUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class ZookeeperUtils {

    private static final Logger log = LoggerFactory.getLogger(DriverUtils.class);


    public static void loadPropertiesFromZookeeper(String authority, Properties properties) {

        String zooKeeperNamespace = HiveDriverProperty.ZOOKEEPER_DISCOVERY_NAMESPACE.get(properties);
        int retry = HiveDriverProperty.ZOOKEEPER_DISCOVERY_RETRY.getInt(properties);

        /*

          example string returned from zookeeper

          hive.server2.authentication=NONE;hive.server2.transport.mode=binary;hive.server2.thrift.sasl.qop=auth;hive.server2.thrift.bind.host=hive-large.hdp.local;hive.server2.thrift.port=10000;hive.server2.use.SSL=false

          hive.server2.authentication=NONE
          hive.server2.transport.mode=binary
          hive.server2.thrift.sasl.qop=auth
          hive.server2.thrift.bind.host=hive-large.hdp.local
          hive.server2.thrift.port=10000
          hive.server2.use.SSL=false

          todo: hive.server2.thrift.http.path
          todo: hive.server2.authentication.kerberos.principal
            current code doesn't like _HOST returned by server
         */


        Random random = new Random();

        try (CuratorFramework zooKeeperClient = CuratorFrameworkFactory.builder().connectString(authority).retryPolicy(new RetryOneTime(retry)).build()) {

            zooKeeperClient.start();

            List<String> hosts = zooKeeperClient.getChildren().forPath('/' + zooKeeperNamespace);

            String randomHost = hosts.get(random.nextInt(hosts.size()));

            String hostData = new String(zooKeeperClient.getData().forPath('/' + zooKeeperNamespace + '/' + randomHost), Charset.forName("UTF-8"));

            Map<String, String> config = Splitter.on(";").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(hostData);

            for (Map.Entry<String, String> entry : config.entrySet()) {
                String value = StringUtils.trimToNull(entry.getValue());

                if (value != null) {

                    String key = entry.getKey();

                    if (log.isDebugEnabled()) {
                        log.debug("found key [{}] and value [{}] in zookeeper", key, value);
                    }

                    HiveDriverProperty hiveDriverProperty = HiveDriverProperty.forAlias(key);

                    if (hiveDriverProperty != null) {
                        hiveDriverProperty.set(properties, value);
                    } else {
                        properties.setProperty(key, value);
                    }
                }
            }


        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

}
