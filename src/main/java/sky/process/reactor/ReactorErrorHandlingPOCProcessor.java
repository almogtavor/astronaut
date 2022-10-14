package sky.process.reactor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Slf4j
@Component
@AllArgsConstructor
@ConditionalOnProperty(prefix = "astronaut.runner", name = "technology", havingValue = "poc")
public class ReactorErrorHandlingPOCProcessor implements ApplicationRunner {
    @Override
    public void run(ApplicationArguments args) {
        Flux.fromIterable(List.of(
                        new Person("Developer", "Jorge", 30),
                        new Person("Developer", "Bob", 32),
                        new Person("Ba", "Ou", 42),
                        new Person("Daaa", "Kim", 54),
                        new Person("Bla", "Bob", 252423)
                ))
                .mapNotNull(p -> {
                    try {
                        if (p.getAge() < 40) {
                            log.info("Age is lower than 40. Throwing exception: " + p);
                            throw new IllegalArgumentException();
                        } else {
                            return p;
                        }
                    } catch (Exception e) {
                        log.info("Catched map not null");
                        return null;
                    }
                })
                .flatMap(p -> {
                    try {
                        if (p.getAge() > 100) {
                            log.info("Age is lower than 40. Throwing exception: " + p);
                            throw new IllegalArgumentException();
                        } else {
                            return Mono.just(p);
                        }
                    } catch (Exception e) {
                        log.info("Catched flatMap");
                        return Mono.empty();
                    }
                })
                .map(msg -> {
                    log.info("This message has passed against the odds: " + msg);
                    return msg;
                })
                .subscribe();
    }

    @Data
    @AllArgsConstructor
    static class Person {
        private String job;
        private String name;
        private Integer age;
    }
}
