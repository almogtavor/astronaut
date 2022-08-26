package sky.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@AllArgsConstructor
@Data
@Document(collection = "bla")
public class File {
    private String itemId;
    private String parentId;
    private Date receptionTime;
    private String hopId;
    private String coolId;
    private String text;
}
