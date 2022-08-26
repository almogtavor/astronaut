package sky.adapters.spark.service;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CepExecutorTest {
    @Test
    void subStatementTest() {
        CepExecutor cepExecutor = Mockito.spy(CepExecutor.class);
        List<String> subStatements = cepExecutor.getSubStatements("SELECT * FROM people WHERE (name=\"Bob\" AND age>21) OR (age<21 OR age>41)");
        Assertions.assertThat(subStatements)
                .hasSameElementsAs(List.of(""));
    }
    @Test
    void givenStatementOfAndClauses_whenCalculatingSubStatements_thenExpectTheRawClauses() {
        CepExecutor cepExecutor = Mockito.spy(CepExecutor.class);
        List<String> subStatements = cepExecutor.getSubStatements("SELECT * FROM people WHERE (name=\"Bob\" AND age>21) OR (age<21 OR age>41)");
//        Assertions.assertThat(subStatements)
//                .hasSameElementsAs(List.of(List.of(""));
    }
}