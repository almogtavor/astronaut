package sky.process.reactor;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import sky.adapters.spark.configuration.SparkConfiguration;
import sky.adapters.spark.service.CepExecutor;

@Component
@AllArgsConstructor
public class ReactorProcessor implements ApplicationRunner {
    private SparkSession sparkSession;
    private CepExecutor cepExecutor;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Flux.from(Mono.just("start"))
                .doOnNext(a -> cepExecutor.executeSqlStatements())
                .subscribe();
    }
}
