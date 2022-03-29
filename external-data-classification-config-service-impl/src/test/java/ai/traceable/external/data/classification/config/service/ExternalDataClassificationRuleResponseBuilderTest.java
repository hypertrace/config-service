package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.OnlyIfChangedFilter;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExternalDataClassificationRuleResponseBuilderTest {
  @Mock UuidGenerator mockUuidGenerator;
  DataType mockDataType = DataType.newBuilder().build();
  GetDataClassificationConfigRequest mockRequest =
      GetDataClassificationConfigRequest.newBuilder()
          .setChangeFilter(OnlyIfChangedFilter.newBuilder().setPreviousHash("previous-hash"))
          .build();

  ExternalDataClassificationRuleResponseBuilder responseBuilder;

  @BeforeEach
  void beforeEach() {
    this.responseBuilder =
        new ExternalDataClassificationRuleResponseBuilder(this.mockUuidGenerator);
  }

  @Test
  void emptyRulesIfMatchHash() {
    GetDataClassificationConfigResponse getResponse =
        GetDataClassificationConfigResponse.newBuilder().addDataTypes(mockDataType).build();
    when(this.mockUuidGenerator.generateId(getResponse))
        .thenReturn(mockRequest.getChangeFilter().getPreviousHash());

    GetDataClassificationConfigResponse response =
        this.responseBuilder.buildResponse(mockRequest, List.of(mockDataType), List.of());
    assertEquals(0, response.getDataParsingRulesCount());
    assertEquals(0, response.getDataTypesCount());
    assertEquals(mockRequest.getChangeFilter().getPreviousHash(), response.getHash());
  }

  @Test
  void returnsRulesIfDifferentHash() {
    GetDataClassificationConfigResponse getResponse =
        GetDataClassificationConfigResponse.newBuilder().addDataTypes(mockDataType).build();
    String differentHash = "different-hash";
    when(this.mockUuidGenerator.generateId(getResponse)).thenReturn(differentHash);

    GetDataClassificationConfigResponse response =
        this.responseBuilder.buildResponse(mockRequest, List.of(mockDataType), List.of());
    assertSame(mockDataType, response.getDataTypes(0));
    assertEquals(0, response.getDataParsingRulesCount());
    assertEquals(differentHash, response.getHash());
  }
}
