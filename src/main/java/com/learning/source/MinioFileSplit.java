package com.learning.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class MinioFileSplit implements SourceSplit {

    private final String bucketName;
    private final String objectKey;

    public MinioFileSplit(String bucketName, String objectKey) {
        this.bucketName = bucketName;
        this.objectKey = objectKey;
    }

    @Override
    public String splitId() {
        return bucketName + "/" + objectKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectKey() {
        return objectKey;
    }
}
