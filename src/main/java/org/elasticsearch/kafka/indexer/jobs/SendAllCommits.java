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

public class SendAllCommits implements ReadCommitStrategy {
private static final Logger logger = LoggerFactory.getLogger(SendAllCommits.class);

Consumer<?,?> kafkaConsumer;
OffsetCommitCallback externalCallback;

public <K,V> SendAllCommits(Consumer<K,V> kafkaConsumer, OffsetCommitCallback externalCallback) {
this.kafkaConsumer = Objects.requireNonNull(kafkaConsumer);
this.externalCallback = Objects.requireNonNull(externalCallback);
}

public void commitOffsetsIfNeeded(boolean shouldCommitThisPoll, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap) {
try {
if (shouldCommitThisPoll) {
long commitStartTime = System.nanoTime();
if (partitionOffsetMap == null) {
kafkaConsumer.commitAsync(externalCallback);
} else {
kafkaConsumer.commitAsync(partitionOffsetMap, externalCallback);
}
long commitTime = System.nanoTime() - commitStartTime;
logger.info("Commit successful for partitions/offsets : {} in {} ns", partitionOffsetMap, commitTime);
} else {
logger.debug("shouldCommitThisPoll = FALSE --> not committing offsets yet");
}
} catch (RetriableCommitFailedException e) {
logger.error("caught RetriableCommitFailedException while committing offsets : {}; abandoning the commit", partitionOffsetMap, e);
}
}
}
