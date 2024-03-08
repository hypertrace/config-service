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
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
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
                    "data.classification.info.cache.max.size", 1000,
                    "data.classification.info.cache.max.thread.pool.size", 2,
                    "data.classification.info.cache.refresh.duration", 10,
                    "data.classification.info.cache.expiration.duration", 20,
                    "data.classification.info.cache.timeout.duration", 5,
                    "data.classification.info.cache.consumer.name",
                        "data-classification-info-cache",
                    "data.classification.info.cache.schema.registry.url",
                        "http://schema-registry-service:8081")));

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
    this.cache =
        new DataClassificationCachingClientImpl(
            this.mockStub, dataClassificationInfoCachingClientConfig);
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
    this.cache.getDataClassificationInfo(testRequestContext, dataTypeFilter);
    dataClassificationInfo =
        this.cache.getDataClassificationInfo(testRequestContext, dataTypeFilter);
    assertEquals(dataTypeIdToDataTypeMap, dataClassificationInfo.getDataTypeIdToDataTypeMap());
    assertEquals(dataSetIdToDataSetMap, dataClassificationInfo.getDataSetIdToDataSetMap());
  }
}
