package veil.hdp.hive.jdbc.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.*;
import java.io.IOException;

public class AnonymousCallbackHandler implements CallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(AnonymousCallbackHandler.class);

    private static String ANONYMOUS = "anonymous";

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nameCallback = (NameCallback) callback;
                nameCallback.setName(ANONYMOUS);
            } else if (callback instanceof PasswordCallback) {
                PasswordCallback passwordCallback = (PasswordCallback) callback;
                passwordCallback.setPassword(ANONYMOUS.toCharArray());
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
