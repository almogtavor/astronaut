package sky.model;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("FakeConsumer")
public class ConsumerPojo {
    @JsonProperty("id")
    private String id;

    public ConsumerPojo(String id) {
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
        return "FakeConsumerDTO{" +
                "id='" + id + '\'' +
                '}';
    }
}
