package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.OnlyIfChangedFilter;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import ai.traceable.external.data.classification.config.service.v1.ObfuscationStrategy;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExternalDataClassificationRuleResponseBuilderTest {
  @Mock UuidGenerator mockUuidGenerator;
  @Mock FeatureCachingClient mockFeatureClient;
  DataType mockDataType = DataType.newBuilder().build();
  GetDataClassificationConfigRequest mockRequest =
      GetDataClassificationConfigRequest.newBuilder()
          .setChangeFilter(OnlyIfChangedFilter.newBuilder().setPreviousHash("previous-hash"))
          .build();

  RequestContext mockRequestContext = RequestContext.forTenantId("response-builder-test");

  @InjectMocks ExternalDataClassificationRuleResponseBuilder responseBuilder;

  @Test
  void emptyRulesIfMatchHash() {
    GetDataClassificationConfigResponse getResponse =
        GetDataClassificationConfigResponse.newBuilder()
            .setEnabled(true)
            .addDataTypes(mockDataType)
            .build();
    when(this.mockUuidGenerator.generateId(getResponse))
        .thenReturn(mockRequest.getChangeFilter().getPreviousHash());

    GetDataClassificationConfigResponse response =
        this.responseBuilder.buildEnabledResponse(
            mockRequest, mockRequestContext, List.of(mockDataType), List.of());
    assertEquals(0, response.getDataParsingRulesCount());
    assertEquals(0, response.getDataTypesCount());
    assertEquals(mockRequest.getChangeFilter().getPreviousHash(), response.getHash());
  }

  @Test
  void returnsRulesIfDifferentHash() {
    GetDataClassificationConfigResponse getResponse =
        GetDataClassificationConfigResponse.newBuilder()
            .setEnabled(true)
            .addDataTypes(mockDataType)
            .build();
    String differentHash = "different-hash";
    when(this.mockUuidGenerator.generateId(getResponse)).thenReturn(differentHash);

    GetDataClassificationConfigResponse response =
        this.responseBuilder.buildEnabledResponse(
            mockRequest, mockRequestContext, List.of(mockDataType), List.of());
    assertSame(mockDataType, response.getDataTypes(0));
    assertEquals(0, response.getDataParsingRulesCount());
    assertEquals(differentHash, response.getHash());
  }

  @Test
  void returnsDisabledResponse() {
    assertFalse(this.responseBuilder.buildDisabledResponse().getEnabled());
  }

  @Test
  void usesAdvancedObfuscationIfEnabled() {
    when(this.mockFeatureClient.isDataClassificationEnhancedObfuscationEnabled(mockRequestContext))
        .thenReturn(false);
    when(this.mockUuidGenerator.generateId(any(GetDataClassificationConfigResponse.class)))
        .thenReturn("some-hash");
    assertFalse(
        this.responseBuilder
            .buildEnabledResponse(mockRequest, mockRequestContext, List.of(mockDataType), List.of())
            .hasObfuscationStrategy());

    when(this.mockFeatureClient.isDataClassificationEnhancedObfuscationEnabled(mockRequestContext))
        .thenReturn(true);

    assertEquals(
        ObfuscationStrategy.newBuilder()
            .setHashFunction(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256)
            .setSalt(mockRequestContext.getTenantId().orElseThrow())
            .build(),
        this.responseBuilder
            .buildEnabledResponse(mockRequest, mockRequestContext, List.of(mockDataType), List.of())
            .getObfuscationStrategy());
  }
}
