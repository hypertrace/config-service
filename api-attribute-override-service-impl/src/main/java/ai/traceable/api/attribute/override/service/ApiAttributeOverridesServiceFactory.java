package ai.traceable.api.attribute.override.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class ApiAttributeOverridesServiceFactory {
  public static BindableService build(ManagedChannel channel) {
    Injector injector = Guice.createInjector(new ApiAttributeOverrideServiceModule(channel));
    return injector.getInstance(BindableService.class);
  }
}
