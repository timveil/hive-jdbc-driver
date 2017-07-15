package veil.hdp.hive.jdbc.core.security;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PrincipalUtils {

    private static final Logger log = LoggerFactory.getLogger(PrincipalUtils.class);


    public static ServicePrincipal parseServicePrincipal(String principal, String hostname) {

        if (StringUtils.isBlank(principal)) {
            throw new RuntimeException("service principal is null or empty");
        }

        if (StringUtils.isBlank(hostname)) {
            throw new RuntimeException("hostname is null or empty");
        }

        List<String> strings = Splitter.on('@').splitToList(principal);

        if (strings.size() != 2) {
            throw new RuntimeException("invalid service principal [" + principal + ']');
        }

        String firstPart = strings.get(0);

        String realm = strings.get(1);


        List<String> serviceParts = Splitter.on('/').splitToList(firstPart);

        if (strings.size() != 2) {
            throw new RuntimeException("invalid first part [" + firstPart + "] of service principal [" + principal + ']');
        }

        String service = serviceParts.get(0);
        String serviceHost = serviceParts.get(1);

        if (serviceHost.equals("0.0.0.0") || serviceHost.equalsIgnoreCase("_HOST")) {
            log.warn("host part [{}] is being replaced by hostname [{}] in the service principal because it is invalid!  Double check configuration", serviceHost, hostname);
            serviceHost = hostname;
        }

        return new ServicePrincipal(service, StringUtils.lowerCase(serviceHost), realm);

    }

    public static UserPrincipal parseUserPrincipal(String principal) {

        if (StringUtils.isBlank(principal)) {
            throw new RuntimeException("user principal is null or empty");
        }

        List<String> strings = Splitter.on('@').splitToList(principal);

        if (strings.size() != 2) {
            throw new RuntimeException("invalid user principal [" + principal + ']');
        }

        String user = strings.get(0);

        String realm = strings.get(1);

        return new UserPrincipal(user, realm);

    }

}
