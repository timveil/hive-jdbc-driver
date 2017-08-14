package veil.hdp.hive.jdbc.utils;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.security.ServicePrincipal;
import veil.hdp.hive.jdbc.security.UserPrincipal;

import java.util.List;

public final class PrincipalUtils {

    private static final Logger log = LoggerFactory.getLogger(PrincipalUtils.class);

    private PrincipalUtils() {
    }


    public static ServicePrincipal parseServicePrincipal(String principal, String hostname) {

        if (StringUtils.isBlank(principal)) {
            throw new IllegalArgumentException("service principal is null or empty");
        }

        if (StringUtils.isBlank(hostname)) {
            throw new IllegalArgumentException("hostname is null or empty");
        }

        List<String> strings = Splitter.on('@').splitToList(principal);

        if (strings.size() != 2) {
            throw new IllegalArgumentException("service principal is invalid [" + principal + ']');
        }

        String firstPart = strings.get(0);

        String realm = strings.get(1);


        List<String> serviceParts = Splitter.on('/').splitToList(firstPart);

        if (strings.size() != 2) {
            throw new IllegalArgumentException("service principal [" + principal + "] has invalid first part [" + firstPart + "].");
        }

        String service = serviceParts.get(0);
        String serviceHost = serviceParts.get(1);

        if (serviceHost.equals("0.0.0.0") || serviceHost.equalsIgnoreCase("_HOST")) {
            log.warn("service principal host part [{}] is being replaced by hostname [{}] because it is invalid.  This can happen if the service principal is retrieved using zookeeper.  If zookeeper discovery is not enabled, double check configuration.", serviceHost, hostname);
            serviceHost = hostname;
        }

        return new ServicePrincipal(service, StringUtils.lowerCase(serviceHost), realm);

    }

    public static UserPrincipal parseUserPrincipal(String principal) {

        if (StringUtils.isBlank(principal)) {
            throw new IllegalArgumentException("user principal is null or empty");
        }

        List<String> strings = Splitter.on('@').splitToList(principal);

        if (strings.size() != 2) {
            throw new IllegalArgumentException("user principal is invalid [" + principal + ']');
        }

        String user = strings.get(0);

        String realm = strings.get(1);

        return new UserPrincipal(user, realm);

    }

}
