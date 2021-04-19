package ai.traceable.customsignature.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class CustomSignatureConfigServiceFactory {
  public static BindableService build(ManagedChannel channel) {
    Injector injector = Guice.createInjector(new CustomSignatureConfigServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
