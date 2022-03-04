package ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig;

import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils;
import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultRateLimitConfigManagerTest {

  private RateLimitConfigManager rateLimitConfigManager;

  @BeforeEach
  void setUp() {
    this.rateLimitConfigManager = new DefaultRateLimitConfigManager(buildConfig());
  }

  @Test
  void getRateLimitConfigTest() {
    // check tenant specific config
    Optional<RateLimitConfig> tenantSpecificRateLimitConfig =
        this.rateLimitConfigManager.getRateLimitConfig(RequestContext.forTenantId("tenant"));
    assertEquals(
        Optional.of(
            SpanProcessingRulesManagerTestUtils.buildExpectedTenantSpecificRateLimitConfig()),
        tenantSpecificRateLimitConfig);

    // check value if tenant specific config is not present
    Optional<RateLimitConfig> defaultRateLimitConfig =
        this.rateLimitConfigManager.getRateLimitConfig(RequestContext.forTenantId("tenant1"));
    assertTrue(defaultRateLimitConfig.isEmpty());
  }
}
