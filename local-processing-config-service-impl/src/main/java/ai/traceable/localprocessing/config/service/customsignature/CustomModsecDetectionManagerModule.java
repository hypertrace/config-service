package ai.traceable.localprocessing.config.service.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class CustomModsecDetectionManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(CustomModsecDetectionManager.class).to(DefaultCustomModsecDetectionManager.class);
  }

  @Provides
  CustomSignatureConfigServiceBlockingStub providesCustomSignatureConfigServiceStub(
      ManagedChannel channel) {
    return CustomSignatureConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
