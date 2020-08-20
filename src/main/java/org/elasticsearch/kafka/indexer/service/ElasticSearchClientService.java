package org.elasticsearch.kafka.indexer.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Created by dhyan on 8/31/15.
 */
// TODO convert to a singleton Spring ES service when ready
@Service
public class ElasticSearchClientService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClientService.class);
    public static final String CLUSTER_NAME = "cluster.name";

    @Value("${elasticsearch.cluster.name:elasticsearch}")
    private String esClusterName;
    @Value("${elasticsearch.cluster.api.keyid:keyidPlaceholder}")
    private String keyId;
    @Value("${elasticsearch.cluster.api.keysecret:secretKeyPlaceholder}")
    private String keySecret;
    
    
    @Value("${elasticsearch.cluster.host:localhost}")
    private String esHost;
   
    @Value("${elasticsearch.cluster.port:9423}")
    private int esPort;
        
    // sleep time in ms between attempts to index data into ES again
    @Value("${elasticsearch.indexing.retry.sleep.ms:10000}")
    private   int esIndexingRetrySleepTimeMs;
    // number of times to try to index data into ES if ES cluster is not reachable
    @Value("${elasticsearch.indexing.retry.attempts:2}")
    private   int numberOfEsIndexingRetryAttempts;

    // TODO add when we can inject partition number into each bean
	//private int currentPartition;
	private RestHighLevelClient esTransportClient;

    @PostConstruct
    public void init() throws Exception {
    	logger.info("Initializing ElasticSearchClient ...");
        // connect to elasticsearch cluster
        Settings settings = Settings.builder().put(CLUSTER_NAME, esClusterName).build();
        try {
        	
			String apiKeyAuth =
			    Base64.getEncoder().encodeToString(
			        (keyId + ":" + keySecret)
			            .getBytes(StandardCharsets.UTF_8));
			RestClientBuilder builder = RestClient.builder(
			    new HttpHost(esHost, esPort, "https"));
			Header[] defaultHeaders =
			    new Header[]{new BasicHeader("Authorization",
			        "ApiKey " + apiKeyAuth)};
			builder.setDefaultHeaders(defaultHeaders);	
			
			
			esTransportClient = new RestHighLevelClient(
				builder);
			
//			IndexRequest request = new IndexRequest("pn-test3");
//			request.source("{\"name\": 100}", XContentType.JSON);
//			esTransportClient.index(request, RequestOptions.DEFAULT);
            
            logger.info("ElasticSearch Client created and intialized OK");
        } catch (Exception e) {
            logger.error("Exception trying to connect and create ElasticSearch Client: "+ e.getMessage());
            throw e;
        }
    }

	@PreDestroy
    public void cleanup() throws Exception {
		//logger.info("About to stop ES client for partition={} ...", currentPartition);
		logger.info("About to stop ES client ...");
		if (esTransportClient != null)
			esTransportClient.close();
    }
    
	public void reInitElasticSearch() throws InterruptedException, IndexerESNotRecoverableException {
		for (int i=1; i<=numberOfEsIndexingRetryAttempts; i++ ){
			Thread.sleep(esIndexingRetrySleepTimeMs);
			logger.warn("Re-trying to connect to ES, try# {} out of {}", i, numberOfEsIndexingRetryAttempts);
			try {
				init();
				// we succeeded - get out of the loop
				return;
			} catch (Exception e) {
				if (i<numberOfEsIndexingRetryAttempts){
					//logger.warn("Re-trying to connect to ES, partition {}, try# {} - failed again: {}", 
					//		currentPartition, i, e.getMessage());						
					logger.warn("Re-trying to connect to ES, try# {} - failed again: {}", 
							i, e.getMessage());						
				} else {
					//we've exhausted the number of retries - throw a IndexerESException to stop the IndexerJob thread
					//logger.error("Re-trying connect to ES, partition {}, "
					//		+ "try# {} - failed after the last retry; Will keep retrying ", currentPartition, i);						
					logger.error("Re-trying connect to ES, try# {} - failed after the last retry", i);						
					//throw new IndexerESException("ERROR: failed to connect to ES after max number of retiries, partition: " +
					//		currentPartition);
					throw new IndexerESNotRecoverableException("ERROR: failed to connect to ES after max number of retries: " + numberOfEsIndexingRetryAttempts);
				}
			}
		}
	}

	public void deleteIndex(String index) {
		DeleteIndexRequest request = new DeleteIndexRequest(index); 
		try {
			esTransportClient.indices().delete(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("Can't delete index " + index);
		}
		logger.info("Delete index {} successfully", index);
	}

	public void createIndex(String indexName){
		CreateIndexRequest request = new CreateIndexRequest(indexName); 
		try {
			esTransportClient.indices().create(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("Can't create index " + indexName);
		}
		logger.info("Created index {} successfully",  indexName);
	}

	public void createIndexAndAlias(String indexName,String aliasName){
		createIndex(indexName);
		IndicesAliasesRequest request = new IndicesAliasesRequest(); 
		AliasActions aliasAction =
		        new AliasActions(AliasActions.Type.ADD)
		        .index(indexName)
		        .alias(aliasName); 
		request.addAliasAction(aliasAction);
		try {
			AcknowledgedResponse indicesAliasesResponse =
					esTransportClient.indices().updateAliases(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("Can't create alias " + aliasName);
		}
		
		logger.info("Created index {} with alias {} successfully" ,indexName,aliasName);
	}

	public void addAliasToExistingIndex(String indexName, String aliasName) {
		IndicesAliasesRequest request = new IndicesAliasesRequest(); 
		AliasActions aliasAction =
		        new AliasActions(AliasActions.Type.ADD)
		        .index(indexName)
		        .alias(aliasName); 
		request.addAliasAction(aliasAction);
		try {
			AcknowledgedResponse indicesAliasesResponse =
					esTransportClient.indices().updateAliases(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("Can't create alias " + aliasName);
		}
		logger.info("Added alias {} to index {} successfully" ,aliasName,indexName);
	}
	




	public BulkResponse executeBulk(BulkRequest bulkRequest) throws IOException {
		return esTransportClient.bulk(bulkRequest, RequestOptions.DEFAULT);
	}

	public RestHighLevelClient getEsTransportClient() {
		return esTransportClient;
	}

}
