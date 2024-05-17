package ai.traceable.data.protection.config.service;

import ai.traceable.data.protection.config.service.v1.DataClassifierCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusion;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusionCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionScope;
import ai.traceable.data.protection.config.service.v1.DataSensitivity;
import ai.traceable.data.protection.config.service.v1.ScopedDataProtectionConfig;
import java.util.List;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataProtectionConfigStoreTest {
  @Mock private ConfigServiceGrpc.ConfigServiceBlockingStub mockStub;
  @Mock private ConfigChangeEventGenerator mockChangeEventGenerator;
  @InjectMocks private DataProtectionConfigStore dataProtectionConfigStore;
  @Mock private ContextualConfigObject<ScopedDataProtectionConfig> first;
  @Mock private ContextualConfigObject<ScopedDataProtectionConfig> second;

  @Test
  void test() {
    ScopedDataProtectionConfig envScopedConfig =
        ScopedDataProtectionConfig.newBuilder()
            .setScope(DataProtectionScope.newBuilder().setEnvironmentId("env"))
            .setConfig(
                DataProtectionConfig.newBuilder()
                    .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_LOW)
                    .addExclusions(
                        DataProtectionExclusion.newBuilder()
                            .addConditions(
                                DataProtectionExclusionCondition.newBuilder()
                                    .setDataClassifierCondition(
                                        DataClassifierCondition.newBuilder()
                                            .addDatasetIds("id2")))))
            .build();
    ScopedDataProtectionConfig tenantScopedConfig =
        ScopedDataProtectionConfig.newBuilder()
            .setConfig(
                DataProtectionConfig.newBuilder()
                    .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH)
                    .addExclusions(
                        DataProtectionExclusion.newBuilder()
                            .addConditions(
                                DataProtectionExclusionCondition.newBuilder()
                                    .setDataClassifierCondition(
                                        DataClassifierCondition.newBuilder()
                                            .addDatasetIds("id1")))))
            .build();

    Mockito.when(first.getData()).thenReturn(envScopedConfig);
    Mockito.when(second.getData()).thenReturn(tenantScopedConfig);

    Assertions.assertEquals(
        List.of(second, first),
        dataProtectionConfigStore.orderFetchedObjects(List.of(first, second)));
  }
}
