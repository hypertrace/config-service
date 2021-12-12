package ai.traceable.risk.config.service.level.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.risk.config.service.level.RiskLevelConfigManager;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfig;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import io.grpc.StatusRuntimeException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RiskLevelConfigManagerTest {
  private RiskLevelConfigValues defaultRiskLevelConfigValues;
  private DefaultObjectStore<RiskLevelConfigValues> configStore;
  private RiskLevelConfigManager riskLevelConfigManager;

  @BeforeEach
  public void setup() {
    defaultRiskLevelConfigValues =
        RiskLevelConfigValues.newBuilder()
            .setHighLevelMinScore(5)
            .setCriticalLevelMinScore(8)
            .build();
    configStore =
        new MockRiskLevelConfigStore(null, null, null, mock(ConfigChangeEventGenerator.class));
    riskLevelConfigManager =
        new RiskLevelConfigManagerImpl(
            configStore, new RiskLevelConfigUtils(), defaultRiskLevelConfigValues);
  }

  @Test
  public void testGetUpdateDeleteRiskLevelConfig() {
    RequestContext requestContext = RequestContext.forTenantId("tenant");

    RiskLevelConfig defaultRiskLevelConfig =
        riskLevelConfigManager.getRiskLevelConfig(requestContext);
    assertTrue(defaultRiskLevelConfig.getIsDefault());
    assertEquals(defaultRiskLevelConfigValues, defaultRiskLevelConfig.getRiskLevelConfigValues());
    assertEquals(
        defaultRiskLevelConfig, riskLevelConfigManager.resetRiskLevelConfig(requestContext));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskLevelConfigManager.updateRiskLevelConfig(
                requestContext,
                RiskLevelConfigValues.newBuilder().setMediumLevelMinScore(4).build()));
    assertEquals(
        defaultRiskLevelConfig,
        riskLevelConfigManager.updateRiskLevelConfig(requestContext, defaultRiskLevelConfigValues));
    RiskLevelConfigValues values =
        RiskLevelConfigValues.newBuilder().setCriticalLevelMinScore(4).build();
    assertEquals(
        RiskLevelConfig.newBuilder().setRiskLevelConfigValues(values).setIsDefault(false).build(),
        riskLevelConfigManager.updateRiskLevelConfig(requestContext, values));
    RiskLevelConfig riskLevelConfig = riskLevelConfigManager.getRiskLevelConfig(requestContext);
    assertFalse(riskLevelConfig.getIsDefault());
    assertEquals(values, riskLevelConfig.getRiskLevelConfigValues());

    assertEquals(
        defaultRiskLevelConfig, riskLevelConfigManager.resetRiskLevelConfig(requestContext));
    assertEquals(defaultRiskLevelConfig, riskLevelConfigManager.getRiskLevelConfig(requestContext));
  }

  static class MockRiskLevelConfigStore extends RiskLevelConfigStore {

    private Map<String, ConfigObject<RiskLevelConfigValues>> values = new HashMap<>();

    protected MockRiskLevelConfigStore(
        ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
        RiskConfigConverter<RiskLevelConfigValues> configConverter,
        RiskConfigUtils<RiskLevelConfigValues> configUtils,
        ConfigChangeEventGenerator configChangeEventGenerator) {
      super(configServiceBlockingStub, configConverter, configUtils, configChangeEventGenerator);
    }

    @Override
    public Optional<RiskLevelConfigValues> getData(RequestContext requestContext) {
      return Optional.ofNullable(values.get(requestContext.getTenantId().get()))
          .map(ConfigObject::getData);
    }

    @Override
    public ConfigObject<RiskLevelConfigValues> upsertObject(
        RequestContext requestContext, RiskLevelConfigValues config) {
      ConfigObject<RiskLevelConfigValues> mock = mock(ConfigObject.class);
      when(mock.getData()).thenReturn(config);
      values.put(requestContext.getTenantId().get(), mock);
      return mock;
    }

    @Override
    public Optional<ConfigObject<RiskLevelConfigValues>> deleteObject(
        RequestContext requestContext) {
      return Optional.ofNullable(values.remove(requestContext.getTenantId().get()));
    }
  }
}
