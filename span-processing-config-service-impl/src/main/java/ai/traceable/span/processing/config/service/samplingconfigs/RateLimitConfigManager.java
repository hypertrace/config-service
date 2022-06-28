package ai.traceable.span.processing.config.service.samplingconfigs;

import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

public class RateLimitConfigManager {

  private final Config rateLimitConfig;
  private static final String RATE_LIMIT_CONFIG_PATH =
      "span.processing.config.service.sampling.config.rate.limit.config";

  @Inject
  public RateLimitConfigManager(Config config) {
    this.rateLimitConfig = config.getConfig(RATE_LIMIT_CONFIG_PATH);
  }

  public RateLimitConfig getRateLimitConfig() {
    try {
      String jsonString = rateLimitConfig.root().render(ConfigRenderOptions.concise());
      RateLimitConfig.Builder builder = RateLimitConfig.newBuilder();
      JsonFormat.parser().merge(jsonString, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
