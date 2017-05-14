package veil.hdp.hive.jdbc.security;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.security.Principal;
import java.util.Map;

public class JdbcLoginModule implements LoginModule {
    private Subject subject;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
    }

    @Override
    public boolean login() throws LoginException {
        return true;
    }

    @Override
    public boolean commit() throws LoginException {

        Principal principal = getCanonicalUser(PlatformUtils.getOsPrincipalClass());

        // todo - need to merge this JdbcPrincipal and ProvidedPrincipal concept;  seems like a bunch of back and forth
        JdbcPrincipal userEntry = new JdbcPrincipal(principal.getName());
        subject.getPrincipals().add(userEntry);

        return true;

    }

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }

    private <T extends Principal> T getCanonicalUser(Class<T> cls) throws LoginException {
        for (T user : subject.getPrincipals(cls)) {
            return user;
        }

        throw new LoginException("Unable to locate principal for class [" + cls + "] in subject [" + subject.toString() + "]");
    }
}
