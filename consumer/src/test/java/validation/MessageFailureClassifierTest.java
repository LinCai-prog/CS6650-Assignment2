
package validation;

import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static validation.MessageFailureClassifier.FailureType.*;

class MessageFailureClassifierTest {

    @Test
    void invalidSwipeEventShouldBePermanent() {

        Exception error =
                new InvalidSwipeEventException(
                        "Vertical must be positive"
                );

        assertEquals(
                PERMANENT,
                MessageFailureClassifier.classify(error)
        );
    }

    @Test
    void malformedJsonShouldBePermanent() {

        Exception error =
                new JsonSyntaxException("Malformed JSON");

        assertEquals(
                PERMANENT,
                MessageFailureClassifier.classify(error)
        );
    }

    @Test
    void otherProcessingFailureShouldBeTransient() {

        Exception error =
                new IllegalStateException(
                        "Processing failure"
                );

        assertEquals(
                TRANSIENT,
                MessageFailureClassifier.classify(error)
        );
    }

    @Test
    void nullExceptionShouldBeRejected() {

        assertThrows(
                IllegalArgumentException.class,
                () -> MessageFailureClassifier.classify(null)
        );
    }
}
