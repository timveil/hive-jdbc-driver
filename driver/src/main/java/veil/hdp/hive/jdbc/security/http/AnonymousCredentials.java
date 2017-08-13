package veil.hdp.hive.jdbc.security.http;

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.auth.Credentials;

import java.security.Principal;

class AnonymousCredentials implements Credentials {

    private static final String ANONYMOUS = "anonymous";

    @Override
    public Principal getUserPrincipal() {
        return new BasicUserPrincipal(ANONYMOUS);
    }

    @Override
    public String getPassword() {
        return ANONYMOUS;
    }
}
