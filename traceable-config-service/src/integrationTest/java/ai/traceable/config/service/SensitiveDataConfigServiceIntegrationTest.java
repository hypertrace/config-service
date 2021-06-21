package ai.traceable.config.service;

import static com.google.common.io.Resources.getResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for SensitiveDataConfigService */
class SensitiveDataConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {

  private static SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceStub;
  private static PiiFilterConfigServiceBlockingStub piiFilterConfigServiceStub;
  private RequestContext requestContext;

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
    requestContext = RequestContext.forTenantId("testSensitiveDataConfigService-tenant");
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
    requestContext = RequestContext.forTenantId("testPiiFilterConfigService-tenant");
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

  @Test
  void testDefaultPopulationConcurrency() throws InterruptedException {
    requestContext = RequestContext.forTenantId("testDefaultPopulationConcurrency-tenant");
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Callable<GetAllRedactionRulesResponse>> calls =
        Stream.generate(() -> (Callable<GetAllRedactionRulesResponse>) this::getRedactionRules)
            .limit(10)
            .collect(Collectors.toList());
    List<Future<GetAllRedactionRulesResponse>> responses = executorService.invokeAll(calls);

    responses.forEach(
        response -> {
          try {
            assertMatchesResource(
                "sensitive-data/get-all-redaction-rules-prepop-response.json", response.get());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void updateRedactionStrategyForType(
      ParamType paramType, RedactionStrategy redactionStrategy) {
    UpdateRedactionStrategyForTypeRequest request =
        UpdateRedactionStrategyForTypeRequest.newBuilder()
            .setParamType(paramType)
            .setRedactionStrategy(redactionStrategy)
            .build();
    requestContext.call(
        () -> sensitiveDataConfigServiceStub.updateRedactionStrategyForType(request));
  }

  private RedactionStrategy getRedactionStrategyForType(ParamType paramType) {
    GetRedactionStrategyForTypeRequest request =
        GetRedactionStrategyForTypeRequest.newBuilder().setParamType(paramType).build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.getRedactionStrategyForType(request))
        .getRedactionStrategy();
  }

  private void updateAutomaticSecretRedactionStrategy(boolean enabled) {
    UpdateAutomaticSecretRedactionStrategyRequest request =
        UpdateAutomaticSecretRedactionStrategyRequest.newBuilder().setEnabled(enabled).build();
    requestContext.call(
        () -> sensitiveDataConfigServiceStub.updateAutomaticSecretRedactionStrategy(request));
  }

  private boolean getAutomaticSecretRedactionStrategy() {
    GetAutomaticSecretRedactionStrategyRequest request =
        GetAutomaticSecretRedactionStrategyRequest.newBuilder().build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.getAutomaticSecretRedactionStrategy(request))
        .getEnabled();
  }

  private RedactionRule createRedactionRule(NewRedactionRule newRedactionRule) {
    CreateRedactionRuleRequest request =
        CreateRedactionRuleRequest.newBuilder().setNewRedactionRule(newRedactionRule).build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.createRedactionRule(request))
        .getRedactionRule();
  }

  private GetAllRedactionRulesResponse getRedactionRules() {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceStub.getAllRedactionRules(
                GetAllRedactionRulesRequest.getDefaultInstance()));
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
    return requestContext
        .call(() -> piiFilterConfigServiceStub.getPiiFilterConfig(request))
        .getPiiFilterConfig();
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

  private void assertMatchesResource(String resourcePath, GetAllRedactionRulesResponse config) {
    GetAllRedactionRulesResponse.Builder builder = GetAllRedactionRulesResponse.newBuilder();
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

  private String toJsonWithIdPlaceholders(GetAllRedactionRulesResponse response)
      throws InvalidProtocolBufferException {
    // This replaces any IDs to placeholders, since they are generated each test
    GetAllRedactionRulesResponse.Builder updatedBuilder = response.toBuilder();

    updatedBuilder.clearRedactionRules();
    updatedBuilder.addAllRedactionRules(
        response.getRedactionRulesList().stream()
            .map(redactionRule -> redactionRule.toBuilder().setId("placeholder-id").build())
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
