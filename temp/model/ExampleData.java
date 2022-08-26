package sky.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@AllArgsConstructor
@Data
@Document(collection = "bla")
public class ExampleData extends org.bson.Document {
    private String itemId;
    private Date receptionTime;
    private String hopId;
    private String coolId;
    private String source;
    private List<File> files;
}
