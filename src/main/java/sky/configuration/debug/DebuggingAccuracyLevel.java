package sky.configuration.debug;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum DebuggingAccuracyLevel {
    /**
     * The leaf statements that a different binary result from them would change the whole statement's result.
     * In case of two failed leaf statements that each of them would lead to the whole statement being passed, Astronaut will return both of them.
     */
    FIND_KEY_STATEMENT("FIND_KEY_STATEMENT"),
    /**
     * The leaf statements that evaluated the unwanted binary result. Regardless their effect on the whole statement.
     */
    FIND_ALL_UNEXPECTED("FIND_ALL_UNEXPECTED");

    @Getter
    private final String value;
}