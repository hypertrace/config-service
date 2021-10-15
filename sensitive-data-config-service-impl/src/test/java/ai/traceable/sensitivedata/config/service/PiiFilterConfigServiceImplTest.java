package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.MockInsightsService;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import io.grpc.Channel;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PiiFilterConfigServiceImplTest {

  PiiFilterConfigServiceBlockingStub piiFilterStub;
  MockGenericConfigService mockGenericConfigService;
  SensitiveDataServiceConfig mockConfig;
  UuidGenerator uuidGenerator = new UuidGenerator();

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    mockConfig = mock(SensitiveDataServiceConfig.class);
    when(mockConfig.defaultParamTypeRedactionStrategy())
        .thenReturn(RedactionStrategy.REDACTION_STRATEGY_HASH);
    when(mockConfig.defaultPiiFilterConfig()).thenReturn(PiiFilterConfig.getDefaultInstance());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void getPiiFilterConfig() {
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    GetPiiFilterConfigResponse response =
        piiFilterStub.getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build());

    PiiFilterConfig piiFilterConfig = response.getPiiFilterConfig();
    assertEquals(uuidGenerator.generateId(piiFilterConfig), response.getHash());
    // Only the prepopulated rules + the ones in mock insights service
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("abc123", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("def456", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement(
                "http.request.header.h1", RedactionStrategy.REDACTION_STRATEGY_HASH, true),
            getPiiElement(
                "http.request.header.h2", RedactionStrategy.REDACTION_STRATEGY_HASH, true));

    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);

    // no update
    response =
        piiFilterStub.getPiiFilterConfig(
            GetPiiFilterConfigRequest.newBuilder().setHash(response.getHash()).build());
    assertEquals(uuidGenerator.generateId(piiFilterConfig), response.getHash());
    assertEquals(
        GetPiiFilterConfigResponse.newBuilder()
            .setHash(uuidGenerator.generateId(piiFilterConfig))
            .build(),
        response);
  }

  @Test
  void getPiiFilterConfigFullPrivacyModeEnabled() {
    setupPiiFilterConfigServiceImpl(true, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // full privacy mode rules + prepopulated rules + mock insights service rules
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("abc123", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("def456", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("uvw789", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("xyz101112", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement(
                "http.request.header.h1", RedactionStrategy.REDACTION_STRATEGY_HASH, true),
            getPiiElement(
                "http.request.header.h2", RedactionStrategy.REDACTION_STRATEGY_HASH, true));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void prepopulatesFilterConfig() {
    setupPiiFilterConfigServiceImpl(false, mock(DefaultRedactionRules.class));

    when(mockConfig.defaultRedactionRules().isPrepopulationComplete(any())).thenReturn(false);
    when(mockConfig.defaultRedactionRules().completedPrepopulationStatus(any()))
        .thenReturn(DefaultRedactionRulePopulationStatus.of(Set.of("key")));
    when(mockConfig.defaultRedactionRules().getRulesToPrepopulate(any()))
        .thenReturn(
            Map.of(
                "other-key",
                NewRedactionRule.newBuilder()
                    .setMatchType(MatchType.MATCH_TYPE_KEY)
                    .setRegex("prepopulated-regex")
                    .build()));

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();

    assertTrue(
        piiFilterConfig.getKeyRegexsList().stream()
            .anyMatch(element -> element.getRegex().equals("prepopulated-regex")));
  }

  private PiiElement getPiiElement(String name, RedactionStrategy redactionStrategy, boolean fqn) {
    return PiiElement.newBuilder()
        .setRegex(name)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(fqn)
        .build();
  }

  private DefaultRedactionRules getDefaultRedactionRules() {
    // prepopulated rules
    Map<String, NewRedactionRule> prepopulationRules =
        Map.of(
            "pre-populated-1",
            NewRedactionRule.newBuilder()
                .setName("Prepopulated Rule 1")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("abc123")
                .build(),
            "pre-populated-2",
            NewRedactionRule.newBuilder()
                .setName("Prepopulated Rule 2")
                .setMatchType(MatchType.MATCH_TYPE_HEADER)
                .setRegex("def456")
                .build());
    // full privacy mode rules
    List<RedactionRule> defaultRedactionRules =
        List.of(
            RedactionRule.newBuilder()
                .setName("Rule 1")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("uvw789")
                .build(),
            RedactionRule.newBuilder()
                .setName("Rule 2")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("xyz101112")
                .build());

    return new DefaultRedactionRules(prepopulationRules, defaultRedactionRules);
  }

  private void setupPiiFilterConfigServiceImpl(
      boolean defaultFullPrivacyMode, DefaultRedactionRules defaultRedactionRules) {
    Channel channel = mockGenericConfigService.channel();
    when(mockConfig.defaultFullPrivacyMode()).thenReturn(defaultFullPrivacyMode);
    when(mockConfig.defaultRedactionRules()).thenReturn(defaultRedactionRules);

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channel);
    mockGenericConfigService
        .addService(new MockInsightsService())
        .addService(
            new PiiFilterConfigServiceImpl(
                mockConfig,
                new ConfigServiceCoordinatorImpl(
                    configServiceBlockingStub,
                    configChangeEventGenerator,
                    mockConfig,
                    new RedactionRuleConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new AutomaticSecretRedactionStrategyConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new FullPrivacyModeConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new DefaultRedactionRulePopulationStatusStore(
                        configServiceBlockingStub, configChangeEventGenerator)),
                new InsightsServiceCoordinatorImpl(InsightsServiceGrpc.newBlockingStub(channel)),
                new UuidGenerator()))
        .start();

    piiFilterStub = PiiFilterConfigServiceGrpc.newBlockingStub(channel);
  }
}
