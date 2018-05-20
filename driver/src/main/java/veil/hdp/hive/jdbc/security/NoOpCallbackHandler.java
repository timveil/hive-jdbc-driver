package veil.hdp.hive.jdbc.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

class NoOpCallbackHandler implements CallbackHandler {

    private static final Logger log = LogManager.getLogger(NoOpCallbackHandler.class);

    @Override
    public void handle(Callback[] callbacks) {
        log.warn("this is not implemented and should not have been called.  this is a problem!");

        for (Callback callback : callbacks) {
            log.warn("callback with class name [{}] is ignored.", callback.getClass().getName());
        }
    }
}
