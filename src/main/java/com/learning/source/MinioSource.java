package com.learning.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class MinioSource implements Source<String, MinioFileSplit, Void> {

    @Override
    public Boundedness getBoundedness() {
        // Our CSV file has a fixed size, so it's bounded
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<String, MinioFileSplit> createReader(SourceReaderContext readerContext) {
        System.out.print("DEBUG: createReader() called - creating MinioSourceReader\n");
        return new MinioSourceReader(readerContext);
    }

    @Override
    public SplitEnumerator<MinioFileSplit, Void> createEnumerator(
            SplitEnumeratorContext<MinioFileSplit> enumContext) {
        System.out.print("DEBUG: createEnumerator() called - creating MinioSplitEnumerator\n");
        return new MinioSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<MinioFileSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<MinioFileSplit> enumContext,
            Void checkpoint) {
        // Restore from checkpoint - for simplicity, just create new one
        return createEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<MinioFileSplit> getSplitSerializer() {
        System.out.println("DEBUG: getSplitSerializer() called - returning non-null serializer");
       return new SimpleVersionedSerializer<MinioFileSplit>() {
           @Override
           public int getVersion() {
               return 1;
           }

           @Override
           public byte[] serialize(MinioFileSplit split) throws IOException {
               ByteArrayOutputStream baos = new ByteArrayOutputStream();
               DataOutputStream out = new DataOutputStream(baos);

               out.writeUTF(split.getBucketName());
               out.writeUTF(split.getObjectKey());

               out.close();

               return baos.toByteArray();
           }

           @Override
           public MinioFileSplit deserialize(int version, byte[] serialized) throws IOException {
               ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
               DataInputStream in = new DataInputStream(bais);

               String bucketName = in.readUTF();
               String objectKey = in.readUTF();

               in.close();
               return new MinioFileSplit(bucketName,objectKey);
           }
       };
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<Void>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Void unused) throws IOException {
                return new byte[0];
            }

            @Override
            public Void deserialize(int i, byte[] bytes) throws IOException {
                return null;
            }
        };
    }
}