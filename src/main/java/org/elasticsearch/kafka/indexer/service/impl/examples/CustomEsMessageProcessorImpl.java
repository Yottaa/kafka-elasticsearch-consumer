/**
  * @author marinapopova
  * Feb 24, 2016
 */
package org.elasticsearch.kafka.indexer.service.impl.examples;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.bigdatadevs.kafkabatch.processor.BatchRecoverableException;
import org.bigdatadevs.kafkabatch.processor.IBatchProcessor;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.elasticsearch.kafka.indexer.service.ElasticSearchBatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * This is an example of a Batch Message Processor that pushes (indexes) events 
 * collected in one poll() to ElasticSearch;
 * You can write your own batch processor that pushes events into some other destination - 
 * for example Cassandra, Postgres, Spark, etc.
 *
 */
public class CustomEsMessageProcessorImpl implements IBatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CustomEsMessageProcessorImpl.class);

    private ElasticSearchBatchService elasticSearchBatchService = null;
	@Value("${elasticsearch.index.name:my_index}")
	private String indexName;
	private String indexPrefix = "test_";

	private ObjectMapper mapper = new ObjectMapper();


    /* (non-Javadoc)
     * @see org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor#processMessage(org.apache.kafka.clients.consumer.ConsumerRecord, int)
     */
    @Override
    public boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord, int consumerId) throws Exception {
        String eventUUID = null; // we don't need a UUID for this simple scenario
        
        String inputMessage = currentKafkaRecord.value();
        if (StringUtils.isEmpty(inputMessage)) {
            return false;
        }
        
        JsonNode fullJsonTree = mapper.readTree(inputMessage);
        
		if (fullJsonTree.get("log_level") == null) 
		return false;
				
		//Separate data in different indices, based on log level
		indexName = indexPrefix + fullJsonTree.get("log_level").asText();
        
		elasticSearchBatchService.addEventToBulkRequest(inputMessage, indexName, eventUUID);
       
		
		return true;
    }


    /* (non-Javadoc)
     * @see org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor#beforeCommitCallBack(int, java.util.Map)
     */
    @Override
    public boolean completePoll(int consumerId, Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition)
            throws BatchRecoverableException, Exception {
        boolean commitOffset = true;
        try {
            elasticSearchBatchService.postToElasticSearch();
        } catch (IndexerESRecoverableException e) {
            // if this is a re-coverable exception - do NOT commit the offsets, let events 
            // from this poll be re-processed
            commitOffset = false;
            logger.error("Recoverable Error posting messages to Elastic Search: {}", e.getMessage());
            throw new BatchRecoverableException("Error posting messages to Elastic Search", e);
        }
        return commitOffset;
    }

    public ElasticSearchBatchService getElasticSearchBatchService() {
        return elasticSearchBatchService;
    }

    public void setElasticSearchBatchService(ElasticSearchBatchService elasticSearchBatchService) {
        this.elasticSearchBatchService = elasticSearchBatchService;
    }


}
