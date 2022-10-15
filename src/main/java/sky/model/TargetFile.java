package sky.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import lombok.extern.jackson.Jacksonized;

import java.util.Date;

@AllArgsConstructor
@Data
@FieldNameConstants
@NoArgsConstructor
@Builder
@Jacksonized
public class TargetFile {
    private String fileId;
    private String parentId;
    private String rootId;
    private String itemType;
    private Date receptionTime;
    private String text;
    private String html;
}
