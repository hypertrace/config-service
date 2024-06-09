package ai.traceable.ratelimiting.service.v2.rules.modsec.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider.DataClassificationInfo;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataClassificationInfoProviderTest {
  @Mock(answer = Answers.RETURNS_SELF)
  DataClassificationConfigServiceBlockingStub mockStub;

  @Mock ClientConfig clientConfig;

  @InjectMocks DataClassificationInfoProvider dataClassificationInfoProvider;

  @Test
  void testDataClassificationInfoFetch() {

    DataType dataType1 =
        DataType.newBuilder()
            .setId("dt1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("Datatype 1")
                    .addDataSetId("ds1")
                    .addDataSetId("ds2"))
            .build();
    DataType dataType2 =
        DataType.newBuilder()
            .setId("dt2")
            .setRule(DataTypeRule.newBuilder().setName("Datatype 2").addDataSetId("ds1"))
            .build();

    when(mockStub.getDataTypes(
            GetDataTypesRequest.newBuilder().setResolveInheritedDetails(true).build()))
        .thenReturn(
            GetDataTypesResponse.newBuilder()
                .addDataTypes(dataType1)
                .addDataTypes(dataType2)
                .build());

    DataClassificationInfo dataClassificationInfo =
        this.dataClassificationInfoProvider.fetchDataClassificationInfo(
            RequestContext.forTenantId("testDataClassificationInfoFetch"));

    assertEquals(List.of("dt1", "dt2"), dataClassificationInfo.getDataTypeIdsForDataSet("ds1"));
    assertEquals(List.of("dt1"), dataClassificationInfo.getDataTypeIdsForDataSet("ds2"));
    assertEquals(Collections.emptyList(), dataClassificationInfo.getDataTypeIdsForDataSet("ds3"));

    assertEquals(dataType1.getRule(), dataClassificationInfo.getDataTypeRule("dt1"));
    assertEquals(dataType2.getRule(), dataClassificationInfo.getDataTypeRule("dt2"));
    assertNull(dataClassificationInfo.getDataTypeRule("dt3"));
  }
}
