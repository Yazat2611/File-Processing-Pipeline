package com.learning.localTesting;

import com.learning.source.MinioSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromSource(
                new MinioSource(),
                WatermarkStrategy.noWatermarks(),
                "Local Minio Source"
        );

        stream.print();

        try {
            env.execute("Local Minio Source Test");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
