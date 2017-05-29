package veil.hdp.hive.jdbc.security;

public class AnonymousCallbackHandler extends UsernamePasswordCallbackHandler {
    private static final String ANONYMOUS = "anonymous";

    public AnonymousCallbackHandler() {
        super(ANONYMOUS, ANONYMOUS);
    }

}
