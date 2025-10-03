package com.learning.source;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MinioSourceReader implements SourceReader<String,MinioFileSplit> {

    private final SourceReaderContext context;
    private BufferedReader reader;
    private MinioFileSplit currentSplit;
    private boolean finished = false;



    public MinioSourceReader(SourceReaderContext context) {
        this.context = context;
    }
    @Override
    public void start() {
        System.out.println("DEBUG: MinioSourceReader.start() called");
    }

    @Override
    public void addSplits(List<MinioFileSplit> splits) {
        System.out.println("DEBUG: addSplits called with " + splits.size() + " splits");
        if(!splits.isEmpty()) {
            this.currentSplit = splits.get(0);
            System.out.println("DEBUG: Split assigned: " + currentSplit.splitId());
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<String> readerOutput) throws Exception {
        System.out.println("DEBUG: pollNext called, currentSplit = " + (currentSplit != null ? currentSplit.splitId() : "null"));
        if (currentSplit == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if(reader == null) {
            openMinioStream();
        }

        String line = reader.readLine();

        if(line!=null) {
            readerOutput.collect(line);
            return InputStatus.MORE_AVAILABLE;
        }

        else {
            closeReader();
            finished = true;
            return InputStatus.END_OF_INPUT;
        }
    }

    @Override
    public List<MinioFileSplit> snapshotState(long checkPointId) {
        List<MinioFileSplit> state = new ArrayList<>();

        if(!finished && currentSplit!=null) {
            state.add(currentSplit);
        }

        return state;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }


    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close(){
        closeReader();
    }

    private void openMinioStream(){

        try {
            System.out.println("Aagya minio connection try krne");
            MinioClient client = MinioClient.builder()
                    .endpoint("http://localhost:9000")
                    .credentials("admin","password123")
                    .build();

            System.out.print("Connection krne ka try kr chuka hai"+ client);

            InputStream inputStream = client.getObject(
                    GetObjectArgs.builder()
                            .bucket(currentSplit.getBucketName())
                            .object(currentSplit.getObjectKey())
                            .build()
            );

            System.out.println("Minion stream try krliya");

            reader = new BufferedReader(new InputStreamReader(inputStream));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to open Minio stream",e);
        }
    }

    private void closeReader() {
       if(reader!=null) {
           try {
               reader.close();
           } catch (IOException e) {
               throw new RuntimeException(e);
           }
           reader = null;
       }
    }

}
