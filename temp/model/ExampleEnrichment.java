package sky.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class ExampleEnrichment extends ExampleBaseModel {
    private String itemId;
    private Date receptionTime;
    private String coolEnrichment;
}
