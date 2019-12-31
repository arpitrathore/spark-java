package org.arpit.spark.structured01.basic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.arpit.spark.common.util.LoggerUtil;
import org.arpit.spark.structured00.common.C02KafkaJsonLogsProducer;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * Guide : https://spark.apache.org/docs/2.4.0/structured-streaming-programming-guide.html
 * Run {@link C02KafkaJsonLogsProducer} to produce apache logs in json format on kafka
 */
public class Structured03Aggregation {

    private static final String APP_NAME = Structured03Aggregation.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", 5) // 200 by default
                .getOrCreate();

        Dataset<Row> dataset = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", C02KafkaJsonLogsProducer.KAFKA_BROKERS)
                .option("subscribe", C02KafkaJsonLogsProducer.KAFKA_TOPIC)
                .option("startingoffsets", "earliest")
                .load();

        StructType structType = new StructType()
                .add("ip", DataTypes.StringType)
                .add("date", DataTypes.StringType)
                .add("verb", DataTypes.StringType)
                .add("resource", DataTypes.StringType)
                .add("responseCode", DataTypes.IntegerType)
                .add("referer", DataTypes.StringType)
                .add("userAgent", DataTypes.StringType)
                .add("responseBytes", DataTypes.LongType);


        Dataset<Row> logDataSet = dataset.select(from_json(col("value")
                .cast(DataTypes.StringType), structType).as("logs")).select("logs.*");
        logDataSet.createOrReplaceTempView("logs");

        Dataset<Row> logsByResponseCode = spark.sql("select responseCode, count(1) as response_count from logs " +
                "group by responseCode order by response_count desc");

        logsByResponseCode.writeStream()
                .format("console")
                .option("truncate", false)
                .outputMode(OutputMode.Complete())
                .start()
                .awaitTermination();
    }

}
