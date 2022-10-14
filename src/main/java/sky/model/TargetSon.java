package sky.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

import java.util.Date;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Jacksonized
public class TargetSon {
    private String id;
    private String itemId;
    private String hopId;
    private String coolId;
    private Date createdDate;
    private List<TargetFile> files;
}
