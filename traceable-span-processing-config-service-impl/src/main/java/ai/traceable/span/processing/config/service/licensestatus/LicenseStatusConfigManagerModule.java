package ai.traceable.span.processing.config.service.licensestatus;

import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class LicenseStatusConfigManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(LicenseStatusConfigManager.class).to(DefaultLicenseStatusConfigManager.class);
  }

  @Provides
  LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub
      providesLicenseStatusConfigServiceBlockingStub(Channel channel) {
    return LicenseStatusConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
