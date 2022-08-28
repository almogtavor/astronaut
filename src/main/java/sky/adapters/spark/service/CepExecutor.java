package sky.adapters.spark.service;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.*;
import org.apache.spark.sql.execution.debug.package$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;
import scala.Option;
import sky.configuration.properties.AstronautProperties;
import sky.configuration.properties.DebugDepthLevel;
import sky.configuration.properties.DebuggingAccuracyLevel;
import sky.configuration.properties.QueuesProperties;
import sky.model.DebugResult;

import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
public class CepExecutor {
    private SparkSession spark;
    private AstronautProperties astronautProperties;
    private QueuesProperties queuesProperties;
    private QueriesInvestigator queriesInvestigator;

    public void executeSqlStatements() {
        // $example on:programmatic_schema$
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("src/main/resources/people.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "name age job";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(";");
            for (int i = 0; i < attributes.length; i++) {
//                String colName = attributes[i].split("\"")[1];
                attributes[i] = attributes[i].trim();
            }
            return RowFactory.create((Object[]) attributes);
        });

        // Convert records of the RDD (people) to Rows

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        Dataset<Row> limit = peopleDataFrame.limit(1);
//        limit.show();
        limit.createOrReplaceTempView("meteor");

        List<String> sqlStatements = List.of(
                "SELECT * FROM meteor WHERE name=\"Ou\" AND (age<31 OR age>50)",
                "SELECT * FROM meteor WHERE name=\"Ou\" OR age<31 OR age>50",
                "SELECT * FROM meteor WHERE name!=\"Bob\"",
                "SELECT * FROM meteor WHERE name=\"Bob\"",
                "SELECT * FROM meteor WHERE (name=\"Bob\" AND age>21) OR (age<21 OR age>41)",
                "SELECT * FROM meteor WHERE name=\"Bob\" AND age>21 OR age<21 OR age>41",
                "SELECT * FROM meteor WHERE name!=\"Bob\" AND age>50"
                // todo: add a case when example
                // todo: add an is not null & is null example
        );

        // this part should run for each document
        for (var statement : sqlStatements) {
            List<String> subStatements = getSubStatements(statement);
            //org.apache.spark.sql.execution.debug.DebugExec.
            Dataset<Row> results = spark.sql(statement);
            results.show();
            // ref: https://mallikarjuna_g.gitbooks.io/spark/content/spark-sql-debugging-execution.html
            // ref: https://books.japila.pl/spark-sql-internals/debugging-query-execution/#demo
            package$.MODULE$.DebugQuery(results).debug();
//            if (results.count() == 0) {
//                List<String> blamedStatements = findBlamedStatements(results);
//            }
            QueryExecution queryExecution = results.queryExecution();
            switch (queryExecution.analyzed()) {
                case Project project -> {
                    switch (project.child()) {
                        case Filter filter -> {
                            List<DebugResult> debugResults = new ArrayList<>();
                            queriesInvestigator.investigateQueryExecution(new QueryInvestigation(debugResults,
                                    peopleDataFrame,
                                    astronautProperties.getDebugMode(),
                                    DebuggingAccuracyLevel.FIND_KEY_STATEMENT,
                                    DebugDepthLevel.LEAF),
                                    filter.condition());
                        }
                        default -> System.out.println("unexpected");
                    }
                }
                default -> System.out.println("CepExecutor.executeSqlStatements");
            }
        }
    }

    private List<String> findBlamedStatements(Dataset<Row> results) {
        results.queryExecution().analyzed().children().toStream().map(child -> {
            child.mapChildren((bla1) ->{
                System.out.println(bla1.metadataOutput());
                System.out.println(bla1.resolved());
                return bla1;
            });
            return child;
        });
//        results.queryExecution().analyzed().innerChildren().toStream().map(child -> {
//            child.conf().
//        })
        return List.of();
    }

    public List<String> getSubStatements(String statement) {
        return null;
    }
}
