package org.elasticsearch.kafka.indexer.jobs;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface ReadCommitStrategy {
    void commitOffsetsIfNeeded(boolean handlerRequestedCommits, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap);

    interface Factory<K,V>{
        ReadCommitStrategy build(Consumer<K,V> kafkaConsumer, OffsetCommitCallback callback);
    }
}
