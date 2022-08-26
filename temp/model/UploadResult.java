package sky.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.http.HttpStatus;

import java.util.List;

@Data
@AllArgsConstructor
@Builder
public class UploadResult {
    HttpStatus status;
    String[] keys;

    public UploadResult() {}

    public UploadResult(HttpStatus status, List<String> keys) {
        this.status = status;
        this.keys = keys == null ? new String[] {}: keys.toArray(new String[] {});

    }
}