package veil.hdp.hive.jdbc.security;

public class AnonymousCallbackHandler extends UsernamePasswordCallbackHandler {
    private static String ANONYMOUS = "anonymous";

    public AnonymousCallbackHandler() {
        super(ANONYMOUS, ANONYMOUS);
    }

}
