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
import java.util.List;
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

        boolean authenticate = true;

        if (cookieStore != null) {
            httpContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

            String cookieName = HiveDriverProperty.HTTP_COOKIE_NAME.get(properties);

            List<Cookie> cookies = cookieStore.getCookies();

            if (cookies != null && !cookies.isEmpty()) {

                for (Cookie cookie : cookies) {
                    log.debug("cookie name [{}], cookie value [{}]", cookie.getName(), cookie.getValue());

                    if (cookie.getName().equalsIgnoreCase(cookieName)) {

                        log.debug("retry cookie [{}] found in CookieStore therefore no need to authenticate again.", cookieName);

                        authenticate = false;
                        break;
                    }

                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("authenticate with kerberos and retrieve ticket [{}]", authenticate);
        }

        if (authenticate) {

            try {

                Subject subject = KerberosService.getSubject(properties);

                String header = Subject.doAs(subject, new PrivilegedExceptionAction<String>() {

                    @Override
                    public String run() throws Exception {

                        String serverPrincipal = HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.get(properties);

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
}
