
package validation;

import com.google.gson.JsonParseException;

public final class MessageFailureClassifier {

    public enum FailureType {
        PERMANENT,
        TRANSIENT
    }

    private MessageFailureClassifier() {
    }

    public static FailureType classify(Throwable error) {

        if (error == null) {
            throw new IllegalArgumentException(
                    "Error cannot be null"
            );
        }

        if (error instanceof JsonParseException
                || error instanceof InvalidSwipeEventException) {
            return FailureType.PERMANENT;
        }

        return FailureType.TRANSIENT;
    }
}
