package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.MockInsightsService;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import io.grpc.Channel;
import java.util.HashSet;
import java.util.Set;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PiiFilterConfigServiceImplTest {

  PiiFilterConfigServiceBlockingStub piiFilterStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    SensitiveDataServiceConfig mockConfig = mock(SensitiveDataServiceConfig.class);
    when(mockConfig.defaultParamTypeRedactionStrategy())
        .thenReturn(RedactionStrategy.REDACTION_STRATEGY_HASH);
    when(mockConfig.defaultPiiFilterConfig()).thenReturn(PiiFilterConfig.getDefaultInstance());
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

  private PiiElement getPiiElement(String name, RedactionStrategy redactionStrategy) {
    return PiiElement.newBuilder()
        .setRegex(name)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(true)
        .build();
  }
}
