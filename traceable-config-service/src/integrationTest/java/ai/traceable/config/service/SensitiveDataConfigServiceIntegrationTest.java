package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.IntegrationTestConfigClientFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for SensitiveDataConfigService */
class SensitiveDataConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {

  private static final String DEFAULT_PII_FILTER_CONFIG =
      "sensitive.data.config.service.default.pii.filter.config";

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

    assertEquals(true, getAutomaticSecretRedactionStrategy());
    updateAutomaticSecretRedactionStrategy(false);
    assertEquals(false, getAutomaticSecretRedactionStrategy());
  }

  @Test
  void testPiiFilterConfigService() throws InvalidProtocolBufferException {
    // automatic secret redaction is disabled and redaction strategy is set to RAW(default)
    updateAutomaticSecretRedactionStrategy(false);
    assertEquals(getExpectedPiiFilterConfig(List.of(), false), getPiiFilterConfig());

    // set redaction strategy to HASH
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    PiiElement piiElement1 =
        getPiiElement(
            "http.request.header.h1", "", RedactionStrategy.REDACTION_STRATEGY_HASH, true, false);
    PiiElement piiElement2 =
        getPiiElement(
            "http.request.header.h2", "", RedactionStrategy.REDACTION_STRATEGY_HASH, true, false);
    PiiFilterConfig expected = getExpectedPiiFilterConfig(List.of(piiElement1, piiElement2), false);
    PiiFilterConfig actual = getPiiFilterConfig();
    assertEquals(expected, actual);

    // enable automatic secret redaction
    updateAutomaticSecretRedactionStrategy(true);
    expected = getExpectedPiiFilterConfig(List.of(piiElement1, piiElement2), true);
    actual = getPiiFilterConfig();
    assertEquals(expected, actual);

    // add redaction rule
    createRedactionRule(
        getNewRedactionRule("rule-1", RedactionStrategy.REDACTION_STRATEGY_REDACT, "^name", false));
    createRedactionRule(
        getNewRedactionRule("rule-2", RedactionStrategy.REDACTION_STRATEGY_HASH, "^address", true));
    PiiElement piiElement3 =
        getPiiElement("^name", "pii", RedactionStrategy.REDACTION_STRATEGY_REDACT, false, false);
    PiiElement piiElement4 =
        getPiiElement("^address", "pii", RedactionStrategy.REDACTION_STRATEGY_HASH, false, true);
    expected =
        getExpectedPiiFilterConfig(
            List.of(piiElement4, piiElement3, piiElement1, piiElement2), true);
    actual = getPiiFilterConfig();
    assertEquals(expected, actual);
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

  private PiiFilterConfig getPiiFilterConfig() {
    GetPiiFilterConfigRequest request = GetPiiFilterConfigRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> piiFilterConfigServiceStub.getPiiFilterConfig(request))
        .getPiiFilterConfig();
  }

  private PiiElement getPiiElement(
      String regex,
      String category,
      RedactionStrategy redactionStrategy,
      boolean isFqn,
      boolean sessionIdentifier) {
    return PiiElement.newBuilder()
        .setRegex(regex)
        .setCategory(category)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(isFqn)
        .setSessionIdentifier(sessionIdentifier)
        .build();
  }

  private PiiFilterConfig getExpectedPiiFilterConfig(
      List<PiiElement> piiElementsAddedByUser, boolean automaticSecretRedactionEnabled)
      throws InvalidProtocolBufferException {
    ConfigClient configClient =
        IntegrationTestConfigClientFactory.getConfigClientForService(SERVICE_NAME);
    Config piiFilterConfig = configClient.getConfig().getConfig(DEFAULT_PII_FILTER_CONFIG);
    String jsonString = piiFilterConfig.root().render(ConfigRenderOptions.concise());
    PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    PiiFilterConfig defaultPiiFilterConfig = builder.build();
    PiiFilterConfig.Builder expectedPiiFilterConfigBuilder =
        PiiFilterConfig.newBuilder()
            .addAllPrefixes(defaultPiiFilterConfig.getPrefixesList())
            .setRedactionStrategy(defaultPiiFilterConfig.getRedactionStrategy());
    expectedPiiFilterConfigBuilder.addAllKeyRegexs(piiElementsAddedByUser);
    if (automaticSecretRedactionEnabled) {
      expectedPiiFilterConfigBuilder.addAllKeyRegexs(defaultPiiFilterConfig.getKeyRegexsList());
      expectedPiiFilterConfigBuilder.addAllValueRegexs(defaultPiiFilterConfig.getValueRegexsList());
      expectedPiiFilterConfigBuilder.addAllComplexData(defaultPiiFilterConfig.getComplexDataList());
    }
    return expectedPiiFilterConfigBuilder.build();
  }
}
