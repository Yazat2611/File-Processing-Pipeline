package com.learning;

import com.learning.source.MinioSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SalesDataProcessor {
    public static void main(String[] args) {

       try {
           System.out.print("DEBUG: Starting SalesDataProcessor Job\n");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            MinioSource source = new MinioSource();

            DataStream<String> rawCsvStream = env.fromSource(
                    source,
                    WatermarkStrategy.noWatermarks(),
                    "MINIO CSV Source"
            );

            rawCsvStream.print();

            env.execute("Sales Data Processing Job");
       }

       catch (Exception e) {
           System.out.print("Error " + e.getMessage());
       }

    }
}

