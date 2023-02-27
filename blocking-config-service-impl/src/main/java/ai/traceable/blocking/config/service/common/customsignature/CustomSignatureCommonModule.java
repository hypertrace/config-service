package ai.traceable.blocking.config.service.common.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class CustomSignatureCommonModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(CustomSignatureBlobFetcher.class).to(DefaultCustomSignatureBlobFetcher.class);
    requireBinding(CustomSignatureConfigServiceBlockingStub.class);
  }
}
