package sky.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("FakeProducer")
public class ProducerPojo {
    @JsonProperty("id")
    private String id;

    public ProducerPojo(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "FakeProducerDTO{" +
                "id='" + id + '\'' +
                '}';
    }
}
