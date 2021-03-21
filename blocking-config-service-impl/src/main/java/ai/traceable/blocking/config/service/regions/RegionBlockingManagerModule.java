package ai.traceable.blocking.config.service.regions;

import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RegionBlockingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RegionBlockingManager.class).to(DefaultRegionBlockingManager.class);
  }

  @Provides
  RegionConfigServiceBlockingStub providesRegionConfigServiceStub(ManagedChannel channel) {
    return RegionConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
