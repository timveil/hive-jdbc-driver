package veil.hdp.hive.jdbc.security.http;


import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

public class BasicRequestInterceptor implements HttpRequestInterceptor {
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException {
        request.addHeader(new BasicScheme().authenticate(new AnonymousCredentials(), request, context));
    }
}
