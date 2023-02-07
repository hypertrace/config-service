package ai.traceable.blocking.config.service.v1.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class CustomModsecBlockingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(CustomModsecBlockingManager.class).to(DefaultCustomModsecBlockingManager.class);
    requireBinding(CustomSignatureConfigServiceBlockingStub.class);
  }
}
