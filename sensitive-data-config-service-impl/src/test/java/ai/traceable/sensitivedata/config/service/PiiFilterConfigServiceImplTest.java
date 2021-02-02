package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.ConfigServiceCoordinatorImpl.DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED;
import static ai.traceable.sensitivedata.config.service.ConfigServiceCoordinatorImpl.DEFAULT_PARAM_TYPE_REDACTION_STRATEGY;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.DEFAULT_PII_FILTER_CONFIG;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.INSIGHTS_SERVICE_CONFIG;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.SENSITIVE_DATA_CONFIG_SERVICE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.config.service.MockInsightsService;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.hypertrace.config.service.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PiiFilterConfigServiceImplTest {

  PiiFilterConfigServiceBlockingStub piiFilterStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet();

    Config config =
        ConfigFactory.parseMap(
            Map.of(
                INSIGHTS_SERVICE_CONFIG,
                Map.of("host", "localhost", "port", 50098),
                SENSITIVE_DATA_CONFIG_SERVICE_CONFIG,
                Map.of(
                    DEFAULT_PARAM_TYPE_REDACTION_STRATEGY,
                    RedactionStrategy.REDACTION_STRATEGY_HASH.name(),
                    DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED,
                    true,
                    DEFAULT_PII_FILTER_CONFIG,
                    Map.of())));
    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService
        .addService(new MockInsightsService())
        .addService(new PiiFilterConfigServiceImpl(channel, channel, config))
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
