
package validation;

public class InvalidSwipeEventException
        extends IllegalArgumentException {

    public InvalidSwipeEventException(String message) {
        super(message);
    }

    public InvalidSwipeEventException(
            String message,
            Throwable cause) {
        super(message, cause);
    }
}
