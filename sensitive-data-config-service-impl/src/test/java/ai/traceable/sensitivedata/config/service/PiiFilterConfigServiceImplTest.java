package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.MockInsightsService;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import io.grpc.Channel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PiiFilterConfigServiceImplTest {

  PiiFilterConfigServiceBlockingStub piiFilterStub;
  MockGenericConfigService mockGenericConfigService;
  SensitiveDataServiceConfig mockConfig;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    mockConfig = mock(SensitiveDataServiceConfig.class);
    when(mockConfig.defaultParamTypeRedactionStrategy())
        .thenReturn(RedactionStrategy.REDACTION_STRATEGY_HASH);
    when(mockConfig.defaultPiiFilterConfig()).thenReturn(PiiFilterConfig.getDefaultInstance());
    when(mockConfig.defaultRedactionRules()).thenReturn(mock(DefaultRedactionRules.class));
    Channel channel = mockGenericConfigService.channel();

    mockGenericConfigService
        .addService(new MockInsightsService())
        .addService(
            new PiiFilterConfigServiceImpl(
                mockConfig,
                new ConfigServiceCoordinatorImpl(
                    ConfigServiceGrpc.newBlockingStub(channel), mockConfig),
                new InsightsServiceCoordinatorImpl(InsightsServiceGrpc.newBlockingStub(channel))))
        .start();

    piiFilterStub = PiiFilterConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void getPiiFilterConfig() {
    when(mockConfig.defaultRedactionRules().isPrepopulationComplete(any())).thenReturn(true);
    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("http.request.header.h1", RedactionStrategy.REDACTION_STRATEGY_HASH),
            getPiiElement("http.request.header.h2", RedactionStrategy.REDACTION_STRATEGY_HASH));
    Set<PiiElement> actual = new HashSet<>(piiFilterConfig.getKeyRegexsList());
    assertEquals(expected, actual);
  }

  @Test
  void prepopulatesFilterConfig() {
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

  private PiiElement getPiiElement(String name, RedactionStrategy redactionStrategy) {
    return PiiElement.newBuilder()
        .setRegex(name)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(true)
        .build();
  }
}
