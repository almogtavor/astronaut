package sky.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@AllArgsConstructor
@Data
public class ExampleEvent {
    private String itemId;
    private Date receptionTime;
}
