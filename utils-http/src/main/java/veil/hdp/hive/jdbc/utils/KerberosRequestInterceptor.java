package veil.hdp.hive.jdbc.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.CookieStore;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.cookie.Cookie;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.security.KerberosService;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;


public class KerberosRequestInterceptor implements HttpRequestInterceptor {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    private static final Base64 BASE_64 = new Base64(0);

    private final Properties properties;
    private final CookieStore cookieStore;

    public KerberosRequestInterceptor(Properties properties, CookieStore cookieStore) {
        this.properties = properties;
        this.cookieStore = cookieStore;
    }

    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {

        String serverPrincipal = HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.get(properties);

        if (cookieStore != null) {
            httpContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

            for (Cookie c : cookieStore.getCookies()) {
                log.debug("cookie name {}, cookie value {}", c.getName(), c.getValue());
            }
        }
/*

        boolean needToSendCredentials = Utils.needToSendCredentials(cookieStore, cookieName, isSSL);

        if (!isCookieEnabled ||
                (
                        (
                                httpContext.getAttribute("hive.server2.retryserver") == null &&
                                        (cookieStore == null || (cookieStore != null && needToSendCredentials))
                        ) ||
                                (
                                        httpContext.getAttribute("hive.server2.retryserver") != null &&
                                                httpContext.getAttribute("hive.server2.retryserver").equals("true")
                                )
                )
                ) {
           // get token
        }
*/

        try {

            Subject subject = KerberosService.getSubject(properties);

            String header = Subject.doAs(subject, new PrivilegedExceptionAction<String>() {

                @Override
                public String run() throws Exception {

                    byte[] token = KerberosService.getToken(serverPrincipal);

                    return new String(BASE_64.encode(token));
                }
            });

            httpRequest.addHeader("Authorization: Negotiate ", header);
        } catch (LoginException | PrivilegedActionException e) {
            log.error(e.getMessage(), e);
        }

    }
}
