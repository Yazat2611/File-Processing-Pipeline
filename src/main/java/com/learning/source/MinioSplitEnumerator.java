package com.learning.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;

public class MinioSplitEnumerator implements SplitEnumerator<MinioFileSplit,Void> {

    private final SplitEnumeratorContext<MinioFileSplit> context;
    private boolean splitAssigned = false;

    public MinioSplitEnumerator(SplitEnumeratorContext<MinioFileSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {
        System.out.println("DEBUG: MinioSplitEnumerator started");
    }

    @Override
    public void handleSplitRequest(int subTaskId, @Nullable String requesterHostname) {

        System.out.println("DEBUG: handleSplitRequest called for subTaskId: " + subTaskId);
        System.out.println("DEBUG: splitAssigned = " + splitAssigned);
        if(!splitAssigned) {
            MinioFileSplit split = new MinioFileSplit("incoming-files","sales-data.csv");
            context.assignSplit(split,subTaskId);
            splitAssigned = true;
        }

        else{
            context.signalNoMoreSplits(subTaskId);
        }
    }

    @Override
    public void addSplitsBack(List<MinioFileSplit> list, int i) {

    }

    @Override
    public void addReader(int subtaskId) {
        System.out.println("DEBUG: addReader called for subtask " + subtaskId);
        if (!splitAssigned) {
            MinioFileSplit split = new MinioFileSplit("incoming-files", "sales-data.csv");
            context.assignSplit(split, subtaskId);
            splitAssigned = true;
            System.out.println("DEBUG: Split assigned to reader " + subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Void snapshotState(long l) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
