package org.elasticsearch.kafka.indexer.service;

import com.google.common.collect.Iterables;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dhyan on 8/31/15.
 */
// TODO convert to a singleton Spring ES service when ready
@Service
public class ElasticSearchClientService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClientService.class);
    public static final String CLUSTER_NAME = "cluster.name";
    private static final String HTTP = "http";

    @Value("${elasticsearch.cluster.name:elasticsearch}")
    private String esClusterName;
    @Value("#{'${elasticsearch.hosts.list:localhost:9300}'.split(',')}")
    private List<String> esHostPortList;
    // sleep time in ms between attempts to index data into ES again
    @Value("${elasticsearch.indexing.retry.sleep.ms:10000}")
    private   int esIndexingRetrySleepTimeMs;
    // number of times to try to index data into ES if ES cluster is not reachable
    @Value("${elasticsearch.indexing.retry.attempts:2}")
    private   int numberOfEsIndexingRetryAttempts;

    // TODO add when we can inject partition number into each bean
	//private int currentPartition;

	private RestHighLevelClient esClient;


    @PostConstruct
    public void init() throws Exception {
    	logger.info("Initializing ElasticSearchClient ...");
        // connect to elasticsearch cluster
        Settings settings = Settings.builder().put(CLUSTER_NAME, esClusterName).build();
        List<HttpHost> hosts = new ArrayList<HttpHost>();


        try {

            for (String eachHostPort : esHostPortList) {
                logger.info("adding [{}] to TransportClient ... ", eachHostPort);

                String[] hostPortTokens = eachHostPort.split(":");
                if (hostPortTokens.length < 2)
                	throw new Exception("ERROR: bad ElasticSearch host:port configuration - wrong format: " +
                		eachHostPort);
                int port = 9200; // default ES port
                try {
                	port = Integer.parseInt(hostPortTokens[1].trim());
                } catch (Throwable e){
                	logger.error("ERROR parsing port from the ES config [{}]- using default port 9300", eachHostPort);
                }
               hosts.add(new HttpHost(hostPortTokens[0].trim(), port, HTTP));

            }

			esClient = new RestHighLevelClient(RestClient.builder(Iterables.toArray(hosts, HttpHost.class)));

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
		if (esClient != null)
			esClient.close();
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

	public void deleteIndex(String index) throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest("posts");
		AcknowledgedResponse deleteIndexResponse = esClient.indices().delete(request, RequestOptions.DEFAULT);
		logger.info("Delete index {} successfully", index);
	}

	public void createIndex(String indexName) throws IOException {
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		AcknowledgedResponse createIndexResponse = esClient.indices().create(request, RequestOptions.DEFAULT);
		logger.info("Created index {} successfully",  indexName);
	}

	public void createIndexAndAlias(String indexName,String aliasName) throws IOException {
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.alias(new Alias(aliasName));
		AcknowledgedResponse createIndexResponse = esClient.indices().create(request, RequestOptions.DEFAULT);

		logger.info("Created index {} with alias {} successfully" ,indexName,aliasName);
	}

	public void addAliasToExistingIndex(String indexName, String aliasName) throws IOException {
		IndicesAliasesRequest request = new IndicesAliasesRequest();
		IndicesAliasesRequest.AliasActions aliasAction =
				new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
						.index(indexName)
						.alias(aliasName);
		request.addAliasAction(aliasAction);
		AcknowledgedResponse addAliasResponse = esClient.indices().updateAliases(request, RequestOptions.DEFAULT);
		logger.info("Added alias {} to index {} successfully" ,aliasName,indexName);
	}

	public void addAliasWithRoutingToExistingIndex(String indexName, String aliasName, String field, String fieldValue) throws IOException {
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.alias(new Alias(aliasName).filter(QueryBuilders.termQuery(field, fieldValue)));
		AcknowledgedResponse createIndexResponse = esClient.indices().create(request, RequestOptions.DEFAULT);
		logger.info("Added alias {} to index {} successfully" ,aliasName,indexName);
	}

	public IndexRequest prepareIndexRequest(String inputMessage, String indexName, String indexType, String eventUUID){
		IndexRequest indexRequest = new IndexRequest().id(eventUUID).type(indexType).source(inputMessage, XContentType.JSON).index(indexName);
    	return indexRequest;
	}

	public RestHighLevelClient getEsClient() {
		return esClient;
	}

}
