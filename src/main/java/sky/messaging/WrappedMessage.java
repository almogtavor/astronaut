package sky.messaging;

import lombok.Builder;
import lombok.Data;
import org.springframework.messaging.Message;

@Builder
@Data
public class WrappedMessage<T> {
    private Message<T> message;
    private Message<Exception> error;

    public boolean isError() {
        return error != null;
    }

    public boolean isContent() {
        return error == null;
    }
}
