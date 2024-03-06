package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeOrdering;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.SystemDataSetVersion;
import ai.traceable.sensitivedata.config.service.ConfigServiceCoordinator.DataClassificationRuleState;
import java.util.List;
import java.util.Set;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigServiceCoordinatorImplTest {
  @Mock ConfigServiceBlockingStub mockGenericStub;
  @Mock ConfigChangeEventGenerator mockChangeEventGenerator;
  @Mock SensitiveDataServiceConfig mockConfig;
  @Mock RedactionRuleConfigStore mockRedactionRuleStore;
  @Mock AutomaticSecretRedactionStrategyConfigStore mockAutoSecretRedactionStore;
  @Mock InvalidJsonPolicyConfigStore mockInvalidJsonPolicyStore;
  @Mock FullPrivacyModeConfigStore mockFullPrivactModeStore;
  @Mock DefaultRedactionRulePersistenceStatusStore mockDefaultRedactionRulePersistenceStore;
  @Mock DataClassificationConfigServiceBlockingStub mockDataClassificationStub;
  RequestContext testContext = RequestContext.forTenantId("ConfigServiceCoordinatorImplTest");
  @InjectMocks ConfigServiceCoordinatorImpl configServiceCoordinator;

  @Test
  void buildsDataClassificationStateCorrectlyWithNoLegacyTypes() {
    DataType nonLegacyDataType =
        DataType.newBuilder()
            .setId("nonlegacy")
            .setRule(
                DataTypeRule.newBuilder().addScopedPatterns(ScopedPattern.getDefaultInstance()))
            .build();

    when(this.mockDataClassificationStub.getDataTypes(
            GetDataTypesRequest.newBuilder()
                .setSystemDataSetVersion(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_RP1)
                .setResolveInheritedDetails(true)
                .setFilter(DataTypeFilter.newBuilder().setEnabled(true))
                .setOrdering(DataTypeOrdering.DATA_TYPE_ORDERING_EVALUATION_PRIORITY)
                .build()))
        .thenReturn(GetDataTypesResponse.newBuilder().addDataTypes(nonLegacyDataType).build());

    assertEquals(
        new DataClassificationRuleState(List.of(nonLegacyDataType), Set.of(), false, false),
        this.configServiceCoordinator.getDataClassificationRuleState(testContext));
  }

  @Test
  void buildsDataClassificationStateCorrectlyWithMixIncludingLegacyTypes() {
    DataType nonLegacyDataType =
        DataType.newBuilder()
            .setId("nonlegacy")
            .setRule(
                DataTypeRule.newBuilder().addScopedPatterns(ScopedPattern.getDefaultInstance()))
            .build();

    DataType legacyType =
        DataType.newBuilder()
            .setId("legacy") // as distinguished by missing rule
            .build();

    DataType autoRedactType =
        DataType.newBuilder().setId(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID).build();

    DataType sensitiveHeaderType =
        DataType.newBuilder().setId(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID).build();

    when(this.mockDataClassificationStub.getDataTypes(
            GetDataTypesRequest.newBuilder()
                .setSystemDataSetVersion(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_RP1)
                .setResolveInheritedDetails(true)
                .setFilter(DataTypeFilter.newBuilder().setEnabled(true))
                .setOrdering(DataTypeOrdering.DATA_TYPE_ORDERING_EVALUATION_PRIORITY)
                .build()))
        .thenReturn(
            GetDataTypesResponse.newBuilder()
                .addDataTypes(nonLegacyDataType)
                .addDataTypes(legacyType)
                .addDataTypes(autoRedactType)
                .addDataTypes(sensitiveHeaderType)
                .build());

    assertEquals(
        new DataClassificationRuleState(
            List.of(nonLegacyDataType), Set.of(legacyType.getId()), true, true),
        this.configServiceCoordinator.getDataClassificationRuleState(testContext));
  }
}
