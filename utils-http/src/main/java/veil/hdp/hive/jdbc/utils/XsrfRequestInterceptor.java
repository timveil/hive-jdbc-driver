package veil.hdp.hive.jdbc.utils;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

public class XsrfRequestInterceptor implements HttpRequestInterceptor {
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
        request.addHeader("X-XSRF-HEADER", "true");
    }
}
