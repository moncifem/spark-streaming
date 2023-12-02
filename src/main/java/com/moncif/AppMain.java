package com.moncif;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class AppMain
{
    public static void main( String[] args ) throws IOException, TimeoutException, StreamingQueryException {
        //1 creer un SparkConf
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark-streaming");
        conf.setMaster("local[*]");

        //2 creer un SparkSession
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        //3 creer un Dataset depuis un socket sur 9999
        /* avec socket
        Dataset<Row> lines = session.readStream()
                .format("socket")
                .option("port","9999")
                .option("host","localhost")
                .load();
                avec kafka
         */
        Dataset<Row> lines = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "TopicTP4")
                .load();
        /*Dataset<Row> words = lines.select("value")
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split("")).iterator(),Encoders.STRING())
                .groupBy("value")
                .count();
        Dataset<String> words = lines.select("value")
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>)  line -> Arrays.asList(line.split(" "))
                        .iterator(),Encoders.STRING());

         */
        Dataset<Row> words = lines.select("value")
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING())
                .map((MapFunction<String, String>) String::toLowerCase, Encoders.STRING())
                .groupBy(col("value"))
                .count();

        //.map(word -> new Tuple2<String, Integer>());
        try{
       /*     StreamingQuery query = words.writeStream()
                    .outputMode("complete")
                    .format("console")
                    .start();
            query.awaitTermination();

        */
            StreamingQuery query = words.writeStream().outputMode(OutputMode.Update())
                    .foreachBatch(
                            // ecrire en base
                            new VoidFunction2<Dataset<Row>, Long>() {
                                @Override
                                public void call(Dataset<Row> ds, Long id) throws Exception {
                                    ds.write().format("org.apache.spark.sql.redis")
                                            .option("spark.redis.host","localhost")
                                            .option("spark.redis.port","6379")
                                            .option("table","words")
                                            .mode(SaveMode.Append)
                                            .save();
                                }

                            }
                    )
                    .start();
            query.awaitTermination();
        }catch (Exception e){
            session.sparkContext().log().error(e.getMessage(),e);
        }

        //4 Compter les mots
        lines.show();

        session.stop();

    }
}
