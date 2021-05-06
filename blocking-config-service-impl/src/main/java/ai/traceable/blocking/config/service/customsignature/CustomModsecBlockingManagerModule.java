package ai.traceable.blocking.config.service.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class CustomModsecBlockingManagerModule extends AbstractModule {

  @Provides
  CustomSignatureConfigServiceBlockingStub providesCustomSignatureConfigServiceStub(
      ManagedChannel channel) {
    return CustomSignatureConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
