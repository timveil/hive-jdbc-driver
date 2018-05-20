package veil.hdp.hive.jdbc.security;

import javax.security.auth.callback.*;
import java.io.IOException;

class UsernamePasswordCallbackHandler implements CallbackHandler {

    private final String username;
    private final String password;

    UsernamePasswordCallbackHandler(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {

        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nameCallback = (NameCallback) callback;
                nameCallback.setName(username);
            } else if (callback instanceof PasswordCallback) {
                PasswordCallback passwordCallback = (PasswordCallback) callback;
                passwordCallback.setPassword(password.toCharArray());
            } else {
                throw new UnsupportedCallbackException(callback, "callback class is not supported [" + callback.getClass().getName() + ']');
            }
        }
    }
}
