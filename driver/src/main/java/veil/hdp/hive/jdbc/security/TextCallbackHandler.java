/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;


public class TextCallbackHandler implements CallbackHandler {

    private static final Logger log = LogManager.getLogger(TextCallbackHandler.class);


    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {

        for (Callback callback : callbacks) {
            if (callback instanceof TextOutputCallback) {

                TextOutputCallback toc = (TextOutputCallback) callback;

                switch (toc.getMessageType()) {
                    case TextOutputCallback.INFORMATION:
                        if (log.isInfoEnabled()) {
                            log.info("from TextCallbackHandler: {}", toc.getMessage());
                        }
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
