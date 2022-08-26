package sky.service;

import org.springframework.messaging.Message;

import java.util.List;

@FunctionalInterface
public interface GroupingPolicyActivator {
    <T> Message<T> apply(List<Message<T>> messages);
}
