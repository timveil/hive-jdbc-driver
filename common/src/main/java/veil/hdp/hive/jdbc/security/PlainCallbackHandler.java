package veil.hdp.hive.jdbc.security;

import javax.security.auth.callback.*;
import java.io.IOException;

public class PlainCallbackHandler implements CallbackHandler {

    private final String username;
    private final String password;

    public PlainCallbackHandler(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nameCallback = (NameCallback) callback;
                nameCallback.setName(username);
            } else if (callback instanceof PasswordCallback) {
                PasswordCallback passwordCallback = (PasswordCallback) callback;

                if (password != null) {
                    passwordCallback.setPassword(password.toCharArray());
                } else {
                    // todo:hack: for some reason this can't be null or empty string; set default value
                    passwordCallback.setPassword("anonymous".toCharArray());
                }

            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
