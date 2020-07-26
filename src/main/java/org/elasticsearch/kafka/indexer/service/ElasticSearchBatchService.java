package org.elasticsearch.kafka.indexer.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by dhyan on 4/11/16.
 */
@Service
@Scope(value = BeanDefinition.SCOPE_PROTOTYPE)
public class ElasticSearchBatchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBatchService.class);
    private static final String SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE";
    private static final String INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR";

    private BulkRequest bulkRequest;
    private Set<String> indexNames = new HashSet<>();
   
    @Value("${elasticsearch.reconnect.attempt.wait.ms:10000}")
    private long sleepBetweenESReconnectAttempts;
    
    @Autowired
    private ElasticSearchClientService elasticSearchClientService;
    IndexRequest indexRequest;

    private void initBulkRequestBuilder(){
    	if (bulkRequest == null){
    		bulkRequest = new BulkRequest();
    	}
    }
    
    /**
     * 
     * @param inputMessage - message body 	
     * @param indexName - ES index name to index this event into 
     * @param indexType - index type of the ES 
     * @param eventUUID - uuid of the event - if needed for routing or as a UUID to use for ES documents; can be NULL
     * @param routingValue - value to use for ES index routing - if needed; can be null if routing is not needed 
     * @throws ExecutionException
     */
    public void addEventToBulkRequest(String inputMessage, String indexName, String indexType, String eventUUID, String routingValue) throws ExecutionException {
    	initBulkRequestBuilder();

    	indexRequest = elasticSearchClientService.prepareIndexRequest(inputMessage,indexName,indexType,eventUUID);

    	if (routingValue != null && routingValue.trim().length()>0) {
            indexRequest.routing(routingValue);
        }
        bulkRequest.add(indexRequest);
        indexNames.add(indexName);
    }

	public void postToElasticSearch() throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException {
		try {
			if (bulkRequest != null) {
				logger.info("Starting bulk posts to ES");

				postBulkToEs(bulkRequest);
				logger.info("Bulk post to ES finished Ok for indexes: {}; # of messages: {}", indexNames, bulkRequest.numberOfActions());
			}
		} finally {
			bulkRequest = null;
			indexNames.clear();
		}
	}

    protected void postBulkToEs(BulkRequest bulkRequest)
            throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException {
        BulkResponse bulkResponse = null;
        BulkItemResponse bulkItemResp = null;
        //Nothing/NoMessages to post to ElasticSearch
        if (bulkRequest.numberOfActions() <= 0) {
            logger.warn("No messages to post to ElasticSearch - returning");
            return;
        }
        try {
            bulkResponse = elasticSearchClientService.getEsClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (NoNodeAvailableException e) {
            // ES cluster is unreachable or down. Re-try up to the configured number of times
            // if fails even after then - throw an exception out to retry indexing the batch
            logger.error("Error posting messages to ElasticSearch: " +
                    "NoNodeAvailableException - ES cluster is unreachable, will try to re-connect after sleeping ... ", e);
            elasticSearchClientService.reInitElasticSearch();
            //even if re-init of ES succeeded - throw an Exception to re-process the current batch
            throw new IndexerESRecoverableException("Recovering after an NoNodeAvailableException posting messages to Elastic Search " +
                    " - will re-try processing current batch");
        } catch (ElasticsearchException | IOException e) {
            logger.error("Failed to post messages to ElasticSearch: " + e.getMessage(), e);
            throw new IndexerESRecoverableException(e);
        }
        logger.debug("Time to post messages to ElasticSearch: {} ms", bulkResponse.getIngestTookInMillis());
        if (bulkResponse.hasFailures()) {
            processFailures(bulkResponse, bulkItemResp);
        }
    }

    private void processFailures(BulkResponse bulkResponse, BulkItemResponse bulkItemResp) throws InterruptedException, IndexerESRecoverableException {
        if (bulkResponse.hasFailures()) {
            logger.error("Bulk Message Post to ElasticSearch has errors: {}",
                    bulkResponse.buildFailureMessage());
            int failedCount = 0;
       
            //TODO research if there is a way to get all failed messages without iterating over
            // ALL messages in this bulk post request
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    failedCount++;

                    String errorMessage = bulkItemResp.getFailure().getMessage();
                    String restResponse = bulkItemResp.getFailure().getStatus().name();
                    logger.error("Failed Message #{}, REST response:{}; errorMessage:{}",
                            failedCount, restResponse, errorMessage);

                    if (SERVICE_UNAVAILABLE.equals(restResponse) || INTERNAL_SERVER_ERROR.equals(restResponse)){
                        logger.error("ES cluster unavailable, thread is sleeping for {} ms, after this current batch will be reprocessed",
                                sleepBetweenESReconnectAttempts);
                        Thread.sleep(sleepBetweenESReconnectAttempts);
                        throw new IndexerESRecoverableException("Recovering after an SERVICE_UNAVAILABLE response from Elastic Search " +
                                " - will re-try processing current batch");
                    }

                    // TODO: there does not seem to be a way to get the actual failed event
                    // until it is possible - do not log anything into the failed events log file
                    //FailedEventsLogger.logFailedToPostToESEvent(restResponse, errorMessage);
                }
            }

            logger.error("FAILURES: # of failed to post messages to ElasticSearch: {} ", failedCount);
        }
    }

    public ElasticSearchClientService getElasticSearchClientService() {
        return elasticSearchClientService;
    }

    public void setElasticSearchClientService(ElasticSearchClientService elasticSearchClientService) {
        this.elasticSearchClientService = elasticSearchClientService;
    }

    protected BulkRequest getBulkRequest() {
        return bulkRequest;
    }

}
