package ai.traceable.config.service;

import static com.google.common.io.Resources.getResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for SensitiveDataConfigService */
class SensitiveDataConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {

  private static SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceStub;
  private static PiiFilterConfigServiceBlockingStub piiFilterConfigServiceStub;

  @BeforeAll
  static void init() {
    sensitiveDataConfigServiceStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    piiFilterConfigServiceStub =
        PiiFilterConfigServiceGrpc.newBlockingStub(managedChannelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testSensitiveDataConfigService() {
    assertEquals(
        RedactionStrategy.REDACTION_STRATEGY_RAW,
        getRedactionStrategyForType(ParamType.PARAM_TYPE_HEADER));
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    assertEquals(
        RedactionStrategy.REDACTION_STRATEGY_HASH,
        getRedactionStrategyForType(ParamType.PARAM_TYPE_HEADER));

    assertTrue(getAutomaticSecretRedactionStrategy());
    updateAutomaticSecretRedactionStrategy(false);
    assertFalse(getAutomaticSecretRedactionStrategy());
  }

  @Test
  void testPiiFilterConfigService() {
    // automatic secret redaction is enabled and redaction strategy is set to RAW(default)
    assertMatchesResource(
        "sensitive-data/pii-filter-with-auto-redaction.json", getPiiFilterConfig(false));

    // set redaction strategy to HASH
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    assertMatchesResource(
        "sensitive-data/pii-filter-with-hash-strategy.json", getPiiFilterConfig(false));

    // disable automatic secret redaction
    updateAutomaticSecretRedactionStrategy(false);
    assertMatchesResource(
        "sensitive-data/pii-filter-without-auto-redaction.json", getPiiFilterConfig(false));
    updateAutomaticSecretRedactionStrategy(true);
    assertMatchesResource(
        "sensitive-data/pii-filter-with-auto-redaction-again.json", getPiiFilterConfig(false));

    // add redaction rule
    createRedactionRule(
        getNewRedactionRule("rule-1", RedactionStrategy.REDACTION_STRATEGY_REDACT, "^name", false));
    createRedactionRule(
        getNewRedactionRule("rule-2", RedactionStrategy.REDACTION_STRATEGY_HASH, "^address", true));
    assertMatchesResource("sensitive-data/pii-filter-after-add.json", getPiiFilterConfig(false));

    // include local processing rules
    assertMatchesResource(
        "sensitive-data/pii-filter-with-local-processing.json", getPiiFilterConfig(true));
  }

  private void updateRedactionStrategyForType(
      ParamType paramType, RedactionStrategy redactionStrategy) {
    UpdateRedactionStrategyForTypeRequest request =
        UpdateRedactionStrategyForTypeRequest.newBuilder()
            .setParamType(paramType)
            .setRedactionStrategy(redactionStrategy)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> sensitiveDataConfigServiceStub.updateRedactionStrategyForType(request));
  }

  private RedactionStrategy getRedactionStrategyForType(ParamType paramType) {
    GetRedactionStrategyForTypeRequest request =
        GetRedactionStrategyForTypeRequest.newBuilder().setParamType(paramType).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> sensitiveDataConfigServiceStub.getRedactionStrategyForType(request))
        .getRedactionStrategy();
  }

  private void updateAutomaticSecretRedactionStrategy(boolean enabled) {
    UpdateAutomaticSecretRedactionStrategyRequest request =
        UpdateAutomaticSecretRedactionStrategyRequest.newBuilder().setEnabled(enabled).build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () -> sensitiveDataConfigServiceStub.updateAutomaticSecretRedactionStrategy(request));
  }

  private boolean getAutomaticSecretRedactionStrategy() {
    GetAutomaticSecretRedactionStrategyRequest request =
        GetAutomaticSecretRedactionStrategyRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () -> sensitiveDataConfigServiceStub.getAutomaticSecretRedactionStrategy(request))
        .getEnabled();
  }

  private RedactionRule createRedactionRule(NewRedactionRule newRedactionRule) {
    CreateRedactionRuleRequest request =
        CreateRedactionRuleRequest.newBuilder().setNewRedactionRule(newRedactionRule).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> sensitiveDataConfigServiceStub.createRedactionRule(request))
        .getRedactionRule();
  }

  private NewRedactionRule getNewRedactionRule(
      String name, RedactionStrategy redactionStrategy, String regex, boolean sessionIdentifier) {
    return NewRedactionRule.newBuilder()
        .setName(name)
        .setDescription("sample rule")
        .setCategory("pii")
        .setRedactionStrategy(redactionStrategy)
        .setMatchType(MatchType.MATCH_TYPE_KEY)
        .setRegex(regex)
        .setSessionIdentifier(sessionIdentifier)
        .build();
  }

  private PiiFilterConfig getPiiFilterConfig(boolean includeConditionalRules) {
    GetPiiFilterConfigRequest request =
        GetPiiFilterConfigRequest.newBuilder()
            .setIncludeConditionalRules(includeConditionalRules)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> piiFilterConfigServiceStub.getPiiFilterConfig(request))
        .getPiiFilterConfig();
  }

  private PiiElement getPiiElement(
      String regex,
      String category,
      RedactionStrategy redactionStrategy,
      boolean isFqn,
      boolean sessionIdentifier,
      String ruleId) {
    return PiiElement.newBuilder()
        .setRegex(regex)
        .setCategory(category)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(isFqn)
        .setSessionIdentifier(sessionIdentifier)
        .setRuleId(ruleId)
        .build();
  }

  private void assertMatchesResource(String resourcePath, PiiFilterConfig config) {
    PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
    try {
      String json = Resources.toString(getResource(resourcePath), StandardCharsets.UTF_8);
      JsonFormat.parser().merge(json, builder);
      String expected = toJsonWithIdPlaceholders(builder.build());
      String actual = toJsonWithIdPlaceholders(config);
      assertEquals(expected, actual);
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private String toJsonWithIdPlaceholders(PiiFilterConfig config)
      throws InvalidProtocolBufferException {
    // This replaces any IDs to placeholders, since they are generated each test
    PiiFilterConfig.Builder updatedBuilder = config.toBuilder();

    updatedBuilder.clearKeyRegexs();
    updatedBuilder.addAllKeyRegexs(
        config.getKeyRegexsList().stream()
            .map(this::usePlaceholderIdIfNeeded)
            .collect(Collectors.toUnmodifiableList()));

    updatedBuilder.clearValueRegexs();
    updatedBuilder.addAllValueRegexs(
        config.getValueRegexsList().stream()
            .map(this::usePlaceholderIdIfNeeded)
            .collect(Collectors.toUnmodifiableList()));

    return JsonFormat.printer().print(updatedBuilder);
  }

  private PiiElement usePlaceholderIdIfNeeded(PiiElement piiElement) {
    if (piiElement.getRuleId().isBlank()) {
      return piiElement;
    }
    return piiElement.toBuilder().setRuleId("placeholder-id").build();
  }
}
