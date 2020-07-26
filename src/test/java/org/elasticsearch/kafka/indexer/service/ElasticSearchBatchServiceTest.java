package org.elasticsearch.kafka.indexer.service;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class ElasticSearchBatchServiceTest {

    @Mock
    private ElasticSearchClientService elasticSearchClientService;

    private ElasticSearchBatchService elasticSearchBatchService = new ElasticSearchBatchService();

    private BulkRequestBuilder mockedBulkRequestBuilder = Mockito.mock(BulkRequestBuilder.class);
    private IndexRequestBuilder mockedIndexRequestBuilder = Mockito.mock(IndexRequestBuilder.class);
    private ListenableActionFuture<BulkResponse> mockedActionFuture = Mockito.mock(ListenableActionFuture.class);
    private BulkResponse mockedBulkResponse = Mockito.mock(BulkResponse.class);
    private String testIndexName = "unitTestsIndex";
    private String testIndexType = "unitTestsType";

    /**
     * @throws java.lang.Exception
     */

    @Before
    public void setUp() throws Exception {
        // Mock all required ES classes and methods
        elasticSearchBatchService.setElasticSearchClientService(elasticSearchClientService);
        //	Mockito.when(elasticSearchClientService.prepareBulk()).thenReturn(mockedBulkRequestBuilder);
        Mockito.when(mockedIndexRequestBuilder.setSource(Matchers.anyString())).thenReturn(mockedIndexRequestBuilder);
        Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);
        Mockito.when(mockedActionFuture.actionGet()).thenReturn(mockedBulkResponse);
        // mock the number of messages in the bulk index request to be 1
        Mockito.when(mockedBulkRequestBuilder.numberOfActions()).thenReturn(1);

        // Mockito.when(elasticIndexHandler.getIndexName(null)).thenReturn(testIndexName);
        // Mockito.when(elasticIndexHandler.getIndexType(null)).thenReturn(testIndexType);
    }

    @Test
    public void testAddEventToBulkRequest_withUUID_withRouting() {
        String message = "test message";
        String eventUUID = "eventUUID";
        // boolean needsRouting = true;
		Mockito.when(elasticSearchClientService.prepareIndexRequest(message, testIndexName, testIndexType, eventUUID)).thenReturn(new IndexRequest().id(eventUUID).type(testIndexType).source(message, XContentType.JSON).index(testIndexName));

		try {
            elasticSearchBatchService.addEventToBulkRequest(message, testIndexName, testIndexType, eventUUID,
                    eventUUID);
        } catch (Exception e) {
            fail("Unexpected exception from unit test: " + e.getMessage());
        }

		Assert.assertEquals(elasticSearchBatchService.getBulkRequest().numberOfActions(),1);
        Mockito.verify(elasticSearchClientService, Mockito.times(1)).prepareIndexRequest(message, testIndexName, testIndexType, eventUUID);
        // verify that routing is set to use eventUUID

    }


}
