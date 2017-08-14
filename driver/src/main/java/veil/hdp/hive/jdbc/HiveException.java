package veil.hdp.hive.jdbc;


public class HiveException extends RuntimeException {

    private static final long serialVersionUID = 1739912754258760020L;

    public HiveException(String message) {
        super(message);
    }

    public HiveException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveException(Throwable cause) {
        super(cause);
    }

}
