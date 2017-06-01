package veil.hdp.hive.jdbc.http;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

public class XsrfRequestInterceptor implements HttpRequestInterceptor {

    private static final String X_XSRF_HEADER = "X-XSRF-HEADER";

    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
        request.addHeader(X_XSRF_HEADER, Boolean.TRUE.toString());
    }
}
