package veil.hdp.hive.jdbc.core.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

;

public class TextCallbackHandler implements CallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(TextCallbackHandler.class);


    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

        for (Callback callback : callbacks) {
            if (callback instanceof TextOutputCallback) {

                TextOutputCallback toc = (TextOutputCallback) callback;

                switch (toc.getMessageType()) {
                    case TextOutputCallback.INFORMATION:
                        log.info("from TextCallbackHandler: {}", toc.getMessage());
                        break;
                    case TextOutputCallback.ERROR:
                        log.error("from TextCallbackHandler: {}", toc.getMessage());
                        break;
                    case TextOutputCallback.WARNING:
                        log.warn("from TextCallbackHandler: {}", toc.getMessage());
                        break;

                }

            } else {
                throw new UnsupportedCallbackException(callback, "callback class is not supported [" + callback.getClass().getName() + ']');
            }
        }
    }
}
