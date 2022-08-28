package sky.configuration.properties;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum DebuggingAccuracyLevel {
    FIND_KEY_STATEMENT("FIND_KEY_STATEMENT"),
    FIND_ALL_UNEXPECTED("FIND_ALL_UNEXPECTED");

    @Getter
    private final String value;
}