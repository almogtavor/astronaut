package sky.engines.spark.service;

import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

public class SparkExtension implements BeforeEachCallback {
  private static SparkSession sparkSession;
  private static JavaSparkContext javaSparkContext;

  private static boolean hasInitSpark = false;

  private static synchronized void setSparkSession(SparkSession sparkSession) {
    SparkExtension.sparkSession = sparkSession;
  }

  private static synchronized void setJavaSparkContext(JavaSparkContext javaSparkContext) {
    SparkExtension.javaSparkContext = javaSparkContext;
  }

  private static synchronized void setHasInitSpark(boolean hasInitSpark) {
    SparkExtension.hasInitSpark = hasInitSpark;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    if (!hasInitSpark) {
      initSpark();
    }
    setHasInitSpark(true);

    setSparkTo(extensionContext);
  }

  private void initSpark() {
    SparkConf sparkConf =
        (new SparkConf())
            .setMaster("local[1]")
            .setAppName("test")
            .set("spark.ui.enabled", "false")
            .set("spark.app.id", UUID.randomUUID().toString())
            .set("spark.driver.host", "localhost")
            .set("spark.sql.shuffle.partitions", "1");

    SparkContext sparkContext = new SparkContext(sparkConf);
    setSparkSession(SparkSession.builder().getOrCreate());

    setJavaSparkContext(new JavaSparkContext(sparkContext));
  }

  private void setSparkTo(ExtensionContext extensionContext) throws IllegalAccessException {
    Object testInstance = extensionContext.getRequiredTestInstance();
    List<Field> fieldsToInject =
        AnnotationSupport.findAnnotatedFields(
            extensionContext.getRequiredTestClass(), SparkSessionField.class);
    for (Field field : fieldsToInject) {
      field.set(testInstance, sparkSession);
    }

    fieldsToInject =
        AnnotationSupport.findAnnotatedFields(
            extensionContext.getRequiredTestClass(), JavaSparkContextField.class);
    for (Field field : fieldsToInject) {
      field.set(testInstance, javaSparkContext);
    }
  }
}
