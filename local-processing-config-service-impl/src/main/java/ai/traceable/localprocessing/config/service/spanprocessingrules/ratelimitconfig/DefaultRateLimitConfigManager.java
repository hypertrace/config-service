package ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig;

import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DefaultRateLimitConfigManager implements RateLimitConfigManager {

  private final Config rateLimitConfig;
  private static final String RATE_LIMIT_CONFIG_PATH =
      "local.processing.config.service.sampling.config.rate.limit.config";

  @Inject
  public DefaultRateLimitConfigManager(Config config) {
    this.rateLimitConfig = config.getConfig(RATE_LIMIT_CONFIG_PATH);
  }

  public Optional<RateLimitConfig> getRateLimitConfig(RequestContext requestContext) {
    String tenantId = requestContext.getTenantId().get();
    boolean tenantSpecificRateLimitRuleExists = hasRateLimitingRuleForTenant(tenantId);
    if (tenantSpecificRateLimitRuleExists) {
      return Optional.of(getRateLimitConfig(this.rateLimitConfig.getConfig(tenantId)));
    }
    return Optional.empty();
  }

  private RateLimitConfig getRateLimitConfig(Config config) {
    try {
      String jsonString = config.root().render(ConfigRenderOptions.concise());
      RateLimitConfig.Builder builder = RateLimitConfig.newBuilder();
      JsonFormat.parser().merge(jsonString, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  boolean hasRateLimitingRuleForTenant(String tenantId) {
    return this.rateLimitConfig.hasPath(tenantId);
  }
}
