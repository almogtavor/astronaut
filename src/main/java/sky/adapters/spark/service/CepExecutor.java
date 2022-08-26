package sky.adapters.spark.service;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
public class CepExecutor {
    private SparkSession spark;

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
        limit.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        List<String> sqlStatements = List.of(
                "SELECT * FROM people WHERE name=\"Ou\" OR age<31 OR age>50",
                "SELECT * FROM people WHERE name=\"Bob\"",
                "SELECT * FROM people WHERE (name=\"Bob\" AND age>21) OR (age<21 OR age>41)",
                "SELECT * FROM people WHERE name=\"Bob\" AND age>21 OR age<21 OR age>41",
                "SELECT * FROM people WHERE name!=\"Bob\" AND age>50"
        );

        // this part should run for each document
        for (var statement : sqlStatements) {
            List<String> subStatements = getSubStatements(statement);
            Dataset<Row> results = spark.sql(statement);
//            if (results.count() == 0) {
//                List<String> blamedStatements = findBlamedStatements(results);
//            }
            QueryExecution queryExecution = results.queryExecution();
            if (queryExecution.analyzed() instanceof Project) {
                Project project = (Project) queryExecution.analyzed();
                if (project.child() instanceof Filter) {
                    Filter filter = (Filter) project.child();
                    if (filter.condition() instanceof EqualTo) {
                        EqualTo equalTo = (EqualTo) filter.condition();
                        System.out.println("The query was: " + equalTo.sql() + " and the resolvation output is: " + equalTo.resolved());
                        peopleDataFrame.sqlContext().sql(equalTo.sql()).show();
                    }
                }
            }
//            System.out.println("Send to X by " + statement);
//            results.show(false);
//            System.out.println("extended:");
//            results.explain(true);
//            System.out.println("ExtendedMode:");
//            results.explain(ExtendedMode.name());
//            System.out.println("CostMode:");
//            results.explain(CostMode.name());
//            System.out.println("FormattedMode:");
//            results.explain(FormattedMode.name());
//            System.out.println("CodegenMode:");
//            results.explain(CodegenMode.name());
//            System.out.println("SimpleMode:");
//            results.explain(SimpleMode.name());
        }

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
//        Dataset<String> namesDS = results.map(
//                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
//                Encoders.STRING());
//        namesDS.show();
        // +-------------+
        // |        value|
        // +-------------+
        // |Name: Michael|
        // |   Name: Andy|
        // | Name: Justin|
        // +-------------+
        // $example off:programmatic_schema$

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
