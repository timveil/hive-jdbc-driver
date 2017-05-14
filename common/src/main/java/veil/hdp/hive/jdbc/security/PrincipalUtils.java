package veil.hdp.hive.jdbc.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public class PrincipalUtils {

    private static final Logger log = LoggerFactory.getLogger(PrincipalUtils.class);

    private static final Pattern PRINCIPAL_PATTERN = Pattern.compile("[/@]");

    public static final ProvidedPrincipal parsePrincipal(String principal) {
        String[] split = PRINCIPAL_PATTERN.split(principal);

        if (split.length == 3) {
            return new ProvidedPrincipal(split[0], split[1], split[2]);
        } else if (split.length == 2) {
            return new ProvidedPrincipal(split[0], getLocalHost(), split[1]);
        } else {
            throw new IllegalArgumentException("invalid principal [" + principal + ']');
        }
    }

    private static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            log.warn(e.getLocalizedMessage(), e);
        }

        return null;
    }
}
