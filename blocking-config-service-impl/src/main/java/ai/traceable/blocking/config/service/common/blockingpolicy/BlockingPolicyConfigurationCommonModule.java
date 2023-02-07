package ai.traceable.blocking.config.service.common.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.DataFetcherModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class BlockingPolicyConfigurationCommonModule extends AbstractModule {
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public BlockingPolicyConfigurationCommonModule(
      Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    install(new DataFetcherModule(config, grpcChannelRegistry));
  }
}
