package sky.messaging;

import lombok.Builder;
import lombok.Data;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;

@Builder
@Data
public class WrappedMessageFactory {
    public static <T> WrappedMessage<T> createSuccessWrappedMessage(T payload, Map<String, Object> headers) {
        return WrappedMessage.<T>builder().message(MessageBuilder.withPayload(payload).copyHeaders(headers).build()).build();
    }
    public static <OldT, NewT> WrappedMessage<NewT> createSuccessWrappedMessage(NewT payload, WrappedMessage<OldT> oldWrappedMessage) {
        if (oldWrappedMessage.isContent()) {
            return WrappedMessage.<NewT>builder().message(MessageBuilder.withPayload(payload).copyHeaders(oldWrappedMessage.getMessage().getHeaders()).build()).build();
        } else {
            return WrappedMessage.<NewT>builder().message(MessageBuilder.withPayload(payload).copyHeaders(oldWrappedMessage.getError().getHeaders()).build()).build();
        }
    }

    public static <T> WrappedMessage<T> createErrorWrappedMessage(Exception exception, Map<String, Object> headers) {
        return WrappedMessage.<T>builder().error(MessageBuilder.withPayload(exception).copyHeaders(headers).build()).build();
    }
    public static <OldT, NewT> WrappedMessage<NewT> createErrorWrappedMessage(Exception exception, WrappedMessage<OldT> oldWrappedMessage) {
        if (oldWrappedMessage.isError()) {
            return WrappedMessage.<NewT>builder().error(MessageBuilder.withPayload(exception).copyHeaders(oldWrappedMessage.getMessage().getHeaders()).build()).build();
        } else {
            return WrappedMessage.<NewT>builder().error(MessageBuilder.withPayload(exception).copyHeaders(oldWrappedMessage.getError().getHeaders()).build()).build();
        }
    }
}
