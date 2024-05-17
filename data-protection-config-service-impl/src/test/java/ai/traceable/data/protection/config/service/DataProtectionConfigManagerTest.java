package ai.traceable.data.protection.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.data.protection.config.service.v1.DataClassifierCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusion;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusionCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionScope;
import ai.traceable.data.protection.config.service.v1.DataSensitivity;
import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.ScopedDataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataProtectionConfigManagerTest {
  private DataProtectionConfigManager configManager;
  private DataProtectionConfigStore configStore;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @BeforeEach
  void setUp() {
    configStore = mock(DataProtectionConfigStore.class);
    configManager = new DataProtectionConfigManager(configStore);
  }

  @Test
  void test_deleteScopedDataProtectionConfig() {
    DeleteScopedDataProtectionConfigRequest request =
        DeleteScopedDataProtectionConfigRequest.newBuilder()
            .setScope(DataProtectionScope.newBuilder().setEnvironmentId("env"))
            .build();
    when(configStore.getContextFromData(request.getScope())).thenReturn("env");
    configManager.deleteScopedDataProtectionConfig(request, requestContext);
    verify(configStore).deleteObject(eq(requestContext), eq("env"));
  }

  @Test
  void test_upsertScopedDataProtectionConfig() {
    UpsertScopedDataProtectionConfigRequest request =
        UpsertScopedDataProtectionConfigRequest.newBuilder()
            .setConfig(
                DataProtectionConfig.newBuilder()
                    .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH))
            .setScope(DataProtectionScope.newBuilder().setEnvironmentId("env"))
            .build();
    ScopedDataProtectionConfig config =
        ScopedDataProtectionConfig.newBuilder()
            .setScope(DataProtectionScope.newBuilder().setEnvironmentId("env"))
            .setConfig(
                DataProtectionConfig.newBuilder()
                    .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_HIGH))
            .build();
    when(configStore.upsertObject(any(), any()))
        .thenReturn(new SampleContextualConfigObject<>(config, "env"));
    Assertions.assertEquals(
        config, configManager.upsertScopedDataProtectionConfig(request, requestContext));
    verify(configStore).upsertObject(eq(requestContext), eq(config));
  }

  @Test
  void test_getResolvedScopedDataProtectionConfig() {
    GetResolvedScopedDataProtectionConfigRequest request =
        GetResolvedScopedDataProtectionConfigRequest.newBuilder().build();
    // no scope
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
    when(configStore.getData(requestContext, "default"))
        .thenReturn(Optional.of(tenantScopedConfig));
    Assertions.assertEquals(
        tenantScopedConfig,
        configManager.getResolvedScopedDataProtectionConfig(request, requestContext).get());

    // with scope no env configs
    request =
        GetResolvedScopedDataProtectionConfigRequest.newBuilder()
            .setScope(DataProtectionScope.newBuilder().setEnvironmentId("env"))
            .build();

    when(configStore.getData(requestContext, "default"))
        .thenReturn(Optional.of(tenantScopedConfig));
    when(configStore.getAllConfigData(requestContext, request.getScope()))
        .thenReturn(List.of(tenantScopedConfig));
    Assertions.assertEquals(
        tenantScopedConfig,
        configManager.getResolvedScopedDataProtectionConfig(request, requestContext).get());

    // with scope
    request =
        GetResolvedScopedDataProtectionConfigRequest.newBuilder()
            .setScope(DataProtectionScope.newBuilder().setEnvironmentId("env"))
            .build();
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

    when(configStore.getData(requestContext, "env")).thenReturn(Optional.of(envScopedConfig));
    when(configStore.getAllConfigData(requestContext, request.getScope()))
        .thenReturn(List.of(tenantScopedConfig, envScopedConfig));
    ScopedDataProtectionConfig expectedConfig =
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
                                        DataClassifierCondition.newBuilder().addDatasetIds("id1"))))
                    .addExclusions(
                        DataProtectionExclusion.newBuilder()
                            .addConditions(
                                DataProtectionExclusionCondition.newBuilder()
                                    .setDataClassifierCondition(
                                        DataClassifierCondition.newBuilder()
                                            .addDatasetIds("id2")))))
            .build();
    Assertions.assertEquals(
        expectedConfig,
        configManager.getResolvedScopedDataProtectionConfig(request, requestContext).get());
  }

  private static class SampleContextualConfigObject<T> implements ContextualConfigObject<T> {

    private final T data;
    private final String context;

    SampleContextualConfigObject(T data, String context) {
      this.data = data;
      this.context = context;
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public Instant getCreationTimestamp() {
      return null;
    }

    @Override
    public Instant getLastUpdatedTimestamp() {
      return null;
    }

    @Override
    public String getContext() {
      return context;
    }
  }
}
