package exception;

public class WriteZNodeDataErrorException extends RuntimeException {
    public WriteZNodeDataErrorException(String message) {
        super(message);
    }

    public WriteZNodeDataErrorException(Throwable cause) {
        super(cause);
    }

    public WriteZNodeDataErrorException(String meaasge, Throwable cause) {
        super(meaasge, cause);
    }
}
