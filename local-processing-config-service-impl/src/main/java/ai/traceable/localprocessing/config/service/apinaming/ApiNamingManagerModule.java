package ai.traceable.localprocessing.config.service.apinaming;

import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ApiNamingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ApiNamingManager.class).to(DefaultApiNamingManager.class);
  }

  @Provides
  TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub
      providesTrainerConfigServiceBlockingStub(ManagedChannel channel) {
    return TrainerConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
