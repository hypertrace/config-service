package ai.traceable.api.gateway.config.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import ai.traceable.platform.cache.TimedCacheConfig;
import com.typesafe.config.Config;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class GatewayConfigServiceCacheConfig {
  private static final String CONNECTION_DETAILS_KEY = "connection";
  @NonNull ConnectionDetails connectionDetails;
  @NonNull Channel channel;
  @NonNull CallCredentials callCredentials;
  @NonNull TimedCacheConfig timedCacheConfig;
  @NonNull Config kafkaConfig;

  @SuppressWarnings("unused")
  public static GatewayConfigServiceCacheConfigBuilder builder(final Config config) {
    return new GatewayConfigServiceCacheConfigBuilder(config);
  }

  @Accessors(fluent = true)
  public static class GatewayConfigServiceCacheConfigBuilder {
    private static final String CACHE_CONFIG_KEY = "cache";
    private static final String CONNECTION_CONFIG_KEY = "connection";
    private static final String KAFKA_CONFIG_KEY = "kafka";

    private final ConnectionDetails connectionDetails;
    private final TimedCacheConfig timedCacheConfig;
    private final Config kafkaConfig;

    @Setter private Channel channel;
    @Setter private CallCredentials callCredentials;

    private GatewayConfigServiceCacheConfigBuilder(final Config config) {
      this.connectionDetails = ConnectionDetails.from(config.getConfig(CONNECTION_CONFIG_KEY));
      this.timedCacheConfig = new TimedCacheConfig(config.getConfig(CACHE_CONFIG_KEY));
      this.kafkaConfig = config.getConfig(KAFKA_CONFIG_KEY);
    }

    @SuppressWarnings("unused")
    public GatewayConfigServiceCacheConfig build() {
      checkNotNull(connectionDetails);
      checkNotNull(channel);
      checkNotNull(callCredentials);
      return new GatewayConfigServiceCacheConfig(
          connectionDetails, channel, callCredentials, timedCacheConfig, kafkaConfig);
    }
  }

  @Value
  @Builder
  static class ConnectionDetails {
    private static final String TIMEOUT_CONFIG_KEY = "timeout";

    Duration timeout;

    static ConnectionDetails from(final Config config) {
      return ConnectionDetails.builder().timeout(config.getDuration(TIMEOUT_CONFIG_KEY)).build();
    }
  }
}
