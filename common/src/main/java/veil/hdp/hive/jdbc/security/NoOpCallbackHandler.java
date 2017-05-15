package veil.hdp.hive.jdbc.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

public class NoOpCallbackHandler implements CallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(NoOpCallbackHandler.class);

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        log.warn("this is not implemented and should not have been called.  this is a problem:  callbacks [{}]", callbacks);
    }
}
