package sky.configuration.properties;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public enum DebugMode {
    ON_FAILURE("ON_FAILURE"),
    ON_SUCCESS("ON_SUCCESS"),
    NEVER("NEVER");

    @Getter
    @Setter
    private String value;
}

