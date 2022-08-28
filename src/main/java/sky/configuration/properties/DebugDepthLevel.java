package sky.configuration.properties;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum DebugDepthLevel {
    HIGHEST_PROBLEMATIC_NODE("HIGHEST_PROBLEMATIC_NODE"),
    LEAF("LEAF");

    @Getter
    private final String value;
}
