package ai.traceable.data.classification.cache.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.cache.config.DataClassificationInfoCachingClientConfig;
import ai.traceable.data.classification.cache.info.DataClassificationInfo;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigDeleteEvent;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.kafka.event.listener.KafkaMockConsumerTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataClassificationCachingClientImplTest {
  @Mock DataClassificationConfigServiceBlockingStub mockStub;
  @Mock DataClassificationConfigServiceBlockingStub ongoingStub;
  RequestContext testRequestContext = RequestContext.forTenantId("DataClassificationInfoCacheTest");
  DataClassificationCachingClientImpl cache;

  @Test
  void test_getDataClassificationInfoMap() {

    DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig =
        DataClassificationInfoCachingClientConfig.from(
            ConfigFactory.parseMap(
                Map.of(
                    "data.classification.info.cache.name",
                    "dataClassificationInfoCache",
                    "data.classification.info.cache.max.size",
                    1000,
                    "data.classification.info.cache.max.thread.pool.size",
                    2,
                    "data.classification.info.cache.refresh.duration",
                    10,
                    "data.classification.info.cache.expiration.duration",
                    20,
                    "data.classification.info.cache.timeout.duration",
                    5)));

    DataType dataType =
        DataType.newBuilder()
            .setId("datatypeId")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype")
                    .addAllDataSetId(List.of("datasetId1", "datasetId2")))
            .build();
    Map<String, DataSet> dataSetIdToDataSetMap =
        Map.of(
            "datasetId1",
                DataSet.newBuilder().setInfo(DataSetInfo.newBuilder().setName("dataset1")).build(),
            "datasetId2",
                DataSet.newBuilder().setInfo(DataSetInfo.newBuilder().setName("dataset2")).build());
    when(this.mockStub.withDeadlineAfter(
            dataClassificationInfoCachingClientConfig.getTimeoutDuration().toMillis(),
            MILLISECONDS))
        .thenReturn(this.ongoingStub);
    when(this.ongoingStub.getDataTypes(
            GetDataTypesRequest.newBuilder().setResolveInheritedDetails(true).build()))
        .thenReturn(
            GetDataTypesResponse.newBuilder()
                .addDataTypes(dataType)
                .putAllReferencedDataSetsById(dataSetIdToDataSetMap)
                .build());
    Config kafkaConfig =
        ConfigFactory.parseMap(
            Map.of(
                "topic.name",
                "mock-config-change-event",
                "poll.timeout",
                "5ms",
                "consumer.name",
                "mock-config-change-event-consumer"));
    KafkaMockConsumerTestUtil<ConfigChangeEventKey, ConfigChangeEventValue> mockConsumerTestUtil =
        new KafkaMockConsumerTestUtil<>("mock-config-change-event", 1);
    MockConsumer<ConfigChangeEventKey, ConfigChangeEventValue> mockConsumer =
        mockConsumerTestUtil.getMockConsumer();
    KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue>
        kafkaLiveEventListenerBuilder =
            new KafkaLiveEventListener.Builder<ConfigChangeEventKey, ConfigChangeEventValue>()
                .build("mock-config-change-event-consumer", kafkaConfig, mockConsumer);
    this.cache =
        new DataClassificationCachingClientImpl(
            kafkaLiveEventListenerBuilder,
            this.mockStub,
            dataClassificationInfoCachingClientConfig);
    DataClassificationInfo dataClassificationInfo =
        this.cache.getDataClassificationInfo(testRequestContext);
    Map<String, DataType> dataTypeIdToDataTypeMap = Map.of("datatypeId", dataType);
    assertEquals(dataTypeIdToDataTypeMap, dataClassificationInfo.getDataTypeIdToDataTypeMap());
    assertEquals(dataSetIdToDataSetMap, dataClassificationInfo.getDataSetIdToDataSetMap());

    GetDataTypesRequest.DataTypeFilter dataTypeFilter =
        GetDataTypesRequest.DataTypeFilter.newBuilder().setEnabled(true).build();
    when(this.ongoingStub.getDataTypes(
            GetDataTypesRequest.newBuilder()
                .setFilter(dataTypeFilter)
                .setResolveInheritedDetails(true)
                .build()))
        .thenReturn(
            GetDataTypesResponse.newBuilder()
                .addDataTypes(dataType)
                .putAllReferencedDataSetsById(dataSetIdToDataSetMap)
                .build());
    dataClassificationInfo =
        this.cache.getDataClassificationInfo(testRequestContext, dataTypeFilter);
    assertEquals(dataTypeIdToDataTypeMap, dataClassificationInfo.getDataTypeIdToDataTypeMap());
    assertEquals(dataSetIdToDataSetMap, dataClassificationInfo.getDataSetIdToDataSetMap());

    DataType dataTypeAfterInvalidation =
        DataType.newBuilder()
            .setId("datatypeIdAfterInvalidation")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatypeAfterInvalidation")
                    .addAllDataSetId(
                        List.of("datasetIdAfterInvalidation1", "datasetIdInvalidation2")))
            .build();

    Map<String, DataSet> dataSetIdToDataSetMapAfterInvalidation =
        Map.of(
            "datasetIdAfterInvalidation1",
            DataSet.newBuilder()
                .setInfo(DataSetInfo.newBuilder().setName("datasetAfterInvalidation1"))
                .build(),
            "datasetIdAfterInvalidation2",
            DataSet.newBuilder()
                .setInfo(DataSetInfo.newBuilder().setName("datasetAfterInvalidation2"))
                .build());
    when(this.ongoingStub.getDataTypes(
            GetDataTypesRequest.newBuilder()
                .setFilter(dataTypeFilter)
                .setResolveInheritedDetails(true)
                .build()))
        .thenReturn(
            GetDataTypesResponse.newBuilder()
                .addDataTypes(dataTypeAfterInvalidation)
                .putAllReferencedDataSetsById(dataSetIdToDataSetMapAfterInvalidation)
                .build());
    mockConsumerTestUtil.addRecord(
        ConfigChangeEventKey.newBuilder()
            .setTenantId("DataClassificationInfoCacheTest")
            .setConfigType(DataType.class.getName())
            .build(),
        ConfigChangeEventValue.newBuilder()
            .setDeleteEvent(ConfigDeleteEvent.newBuilder().build())
            .build());
    DataClassificationInfo dataClassificationInfo1 =
        this.cache.getDataClassificationInfo(testRequestContext, dataTypeFilter);
    assertEquals(
        Map.of("datatypeIdAfterInvalidation", dataTypeAfterInvalidation),
        dataClassificationInfo1.getDataTypeIdToDataTypeMap());
    assertEquals(
        dataSetIdToDataSetMapAfterInvalidation, dataClassificationInfo1.getDataSetIdToDataSetMap());
  }
}
