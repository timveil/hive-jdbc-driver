package veil.hdp.hive.jdbc.security;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PrincipalUtils {

    private static final Logger log = LoggerFactory.getLogger(PrincipalUtils.class);


    public static ServicePrincipal parseServicePrincipal(String principal) {

        if (StringUtils.isBlank(principal)) {
            throw new RuntimeException("principal is null or empty");
        }

        List<String> strings = Splitter.on('@').splitToList(principal);

        if (strings.size() != 2) {
            throw new RuntimeException("invalid principal [" + principal + ']');
        }

        String firstPart = strings.get(0);

        String realm = strings.get(1);


        List<String> serviceParts = Splitter.on('/').splitToList(firstPart);

        if (strings.size() != 2) {
            throw new RuntimeException("invalid first part [" + firstPart + "] of principal [" + principal + ']');
        }

        String service = serviceParts.get(0);
        String host = serviceParts.get(1);

        return new ServicePrincipal(service, StringUtils.lowerCase(host), realm);

    }

    public static UserPrincipal parseUserPrincipal(String principal) {

        if (StringUtils.isBlank(principal)) {
            throw new RuntimeException("principal is null or empty");
        }

        List<String> strings = Splitter.on('@').splitToList(principal);

        if (strings.size() != 2) {
            throw new RuntimeException("invalid principal [" + principal + ']');
        }

        String user = strings.get(0);

        String realm = strings.get(1);

        return new UserPrincipal(user, realm);

    }

}
