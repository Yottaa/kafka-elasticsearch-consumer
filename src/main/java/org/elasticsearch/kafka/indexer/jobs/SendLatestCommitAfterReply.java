package org.elasticsearch.kafka.indexer.jobs;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;


/**
 * ReadCommitStrategy that tries to limit the number of in-flight commits, so we won't flood kafka/zookeeper when
 * we have many consumers trying to checkpoint a large volume of batches with only a few entries.  With this approach,
 * when the IConsumerWorker indicates the latest set of batches should be completed, rather than sending them immediately
 * to kafka, the following approach is used:
 *
 * 1. if there are no read commits already in flight, the current offsets are immediately sent to kafka (similar to previous approach)
 * 2. if there are read commits in flight, the current offsets are stashed, replacing any offsets that may have been
 *       previously stashed.
 * 4. When the result of a read-commit in flight arrives, if the stashed offsets are different than the the last sent offsets,
 *       they are sent to kafka.  Note that if no more messages are read from kafka and the last read-commit fails, this strategy will
 *       not attempt to retry the commit.
 *
 * This approach ensures that each consumer will have at most one read commit in flight to kafka, and will always send the latest
 * offsets the IConsumerWorker indicated were ready to commit.  At low volumes where commits can return before the
 * next batch arrives, this strategy will behave exactly like the previous default strategy, and send every
 * committed offset immediately.  When the batch arrival rate exceeds the read commit processing rate of the kafka servers,
 * this approach will avoid flooding the servers with intermediate commits that are not as important as the most recent commits.
 */
public class SendLatestCommitAfterReply implements ReadCommitStrategy, OffsetCommitCallback {
    private static final Logger logger = LoggerFactory.getLogger(SendLatestCommitAfterReply.class);

    Consumer<?, ?> kafkaConsumer;
    OffsetCommitCallback externalResultCallback;
    //NOTE: thread safety of this logic requires onComplete() and commitOffsetsIfNeeded() calls to be serialized externally, IE: by the kafka consumer thread.
    Map<TopicPartition, OffsetAndMetadata> latestHandlerCommitedOffsets = null;
    Map<TopicPartition, OffsetAndMetadata> lastSentOffset = null;
    Map<TopicPartition, OffsetAndMetadata> inflightOffsets = null;

    //these variables are not actively used by the algorithm, but can be very useful when in the debugger, or a heap dump
    Map<TopicPartition, OffsetAndMetadata> latestSeenOffsets = null;
    Map<TopicPartition, OffsetAndMetadata> lastSuccessOffsets = null;
    Map<TopicPartition, OffsetAndMetadata> lastFailOffsets = null;

    public SendLatestCommitAfterReply(Consumer<?, ?> kafkaConsumer, OffsetCommitCallback externalResultCallback) {
        this.kafkaConsumer = Objects.requireNonNull(kafkaConsumer);
        this.externalResultCallback = Objects.requireNonNull(externalResultCallback);
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        try {
            if (exception != null) {
                lastFailOffsets = offsets;
            } else {
                lastSuccessOffsets = offsets;
            }
            inflightOffsets = null; //we got a result, so clear the inflight, allowing pending to get evaluated.
            sendPendingCommitsIfPossible();
        } finally {
            try {
                externalResultCallback.onComplete(offsets, exception);
            } catch (Exception e){
                //ignore, not anything we can do about it, and it's probably a logging callback.
            }
        }
    }

    @Override
    public void commitOffsetsIfNeeded(boolean handlerRequestedCommits, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap) {
        this.latestSeenOffsets = partitionOffsetMap;
        if (handlerRequestedCommits){
            this.latestHandlerCommitedOffsets = partitionOffsetMap;
        }
        if (inflightOffsets != null){
            logger.info("Already have inflight commit, staging potential commit for partitions/offsets : {}", inflightOffsets);
        }
        sendPendingCommitsIfPossible();
    }

    private boolean recursed = false;

    private void sendPendingCommitsIfPossible() {
        if (recursed)
            return;  //this check is here, to deal with unit tests that might invoke a result before the previous send is complete.

        if (inflightOffsets == null && latestHandlerCommitedOffsets != null && lastSentOffset!=latestHandlerCommitedOffsets){
            try {
                recursed = true;
                kafkaConsumer.commitAsync(latestHandlerCommitedOffsets,this);
                lastSentOffset = latestHandlerCommitedOffsets;
                inflightOffsets = latestHandlerCommitedOffsets;
                logger.info("Sent commit for partitions/offsets : {}", inflightOffsets);
            } catch (RetriableCommitFailedException e) {
                logger.error("caught RetriableCommitFailedException while sending commit offsets : {}; abandoning the commit", latestHandlerCommitedOffsets, e);
            } finally {
                recursed = false;
            }
        }
    }
}
