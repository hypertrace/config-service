package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.ConfigServiceCoordinatorImpl.DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED;
import static ai.traceable.sensitivedata.config.service.ConfigServiceCoordinatorImpl.DEFAULT_PARAM_TYPE_REDACTION_STRATEGY;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.SENSITIVE_DATA_CONFIG_SERVICE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.hypertrace.config.service.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SensitiveDataConfigServiceImplTest {
  SensitiveDataConfigServiceBlockingStub sensitiveDataStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet();

    Config config =
        ConfigFactory.parseMap(
            Map.of(
                SENSITIVE_DATA_CONFIG_SERVICE_CONFIG,
                Map.of(
                    DEFAULT_PARAM_TYPE_REDACTION_STRATEGY,
                    RedactionStrategy.REDACTION_STRATEGY_RAW.name(),
                    DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED,
                    true)));
    mockGenericConfigService
        .addService(new SensitiveDataConfigServiceImpl(mockGenericConfigService.channel(), config))
        .start();

    sensitiveDataStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void upsertAndGetRedactionStrategyForType() {
    RedactionStrategy redactionStrategy =
        sensitiveDataStub
            .getRedactionStrategyForType(
                GetRedactionStrategyForTypeRequest.newBuilder()
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .build())
            .getRedactionStrategy();
    assertEquals(RedactionStrategy.REDACTION_STRATEGY_RAW, redactionStrategy);

    sensitiveDataStub.updateRedactionStrategyForType(
        UpdateRedactionStrategyForTypeRequest.newBuilder()
            .setParamType(ParamType.PARAM_TYPE_HEADER)
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
            .build());
    redactionStrategy =
        sensitiveDataStub
            .getRedactionStrategyForType(
                GetRedactionStrategyForTypeRequest.newBuilder()
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .build())
            .getRedactionStrategy();
    assertEquals(RedactionStrategy.REDACTION_STRATEGY_REDACT, redactionStrategy);
  }

  @Test
  void upsertAndGetAutomaticSecretRedactionStrategy() {
    boolean automaticSecretRedactionEnabled =
        sensitiveDataStub
            .getAutomaticSecretRedactionStrategy(
                GetAutomaticSecretRedactionStrategyRequest.newBuilder().build())
            .getEnabled();
    assertEquals(true, automaticSecretRedactionEnabled);

    sensitiveDataStub.updateAutomaticSecretRedactionStrategy(
        UpdateAutomaticSecretRedactionStrategyRequest.newBuilder().setEnabled(false).build());
    automaticSecretRedactionEnabled =
        sensitiveDataStub
            .getAutomaticSecretRedactionStrategy(
                GetAutomaticSecretRedactionStrategyRequest.newBuilder().build())
            .getEnabled();
    assertEquals(false, automaticSecretRedactionEnabled);
  }
}
