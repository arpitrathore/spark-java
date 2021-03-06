package org.arpit.spark.structured01.basic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.arpit.spark.common.util.LoggerUtil;
import org.arpit.spark.structured00.common.C01KafkaRawLogsProducer;
import org.arpit.spark.structured00.common.C02KafkaJsonLogsProducer;

/**
 * Guide : https://spark.apache.org/docs/2.4.0/structured-streaming-programming-guide.html
 * Run {@link C02KafkaJsonLogsProducer} to produce apache logs in json format on kafka
 */
public class Structured01Basic {

    private static final String APP_NAME = Structured01Basic.class.getName();

    public static void main(String[] args) throws Exception {
        LoggerUtil.disableSparkLogs();

        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", C02KafkaJsonLogsProducer.KAFKA_BROKERS)
                .option("subscribe", C02KafkaJsonLogsProducer.KAFKA_TOPIC)
                .option("startingoffsets", "earliest")
                .load();

        dataset.printSchema();

        Dataset<Row> valueDataset = dataset.select(col("value").cast(DataTypes.StringType));

        valueDataset.writeStream()
                .format("console")
                .option("truncate", false)
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();
    }

}
