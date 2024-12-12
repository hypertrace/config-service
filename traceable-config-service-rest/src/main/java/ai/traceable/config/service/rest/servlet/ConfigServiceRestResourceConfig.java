package ai.traceable.config.service.rest.servlet;

import static ai.traceable.config.service.rest.TraceableConfigServiceRestConfig.ChannelAndApiSpecsConfig;
import static ai.traceable.config.service.rest.TraceableConfigServiceRestConfig.GrpcApiSpecsConfig;

import ai.traceable.config.service.rest.TraceableConfigServiceRestConfig;
import ai.traceable.config.service.rest.codegen.GrpcStubGenerator;
import ai.traceable.config.service.rest.codegen.JaxRsResourceGenerator;
import ai.traceable.config.service.rest.codegen.error.GrpcStubGenerationException;
import ai.traceable.config.service.rest.codegen.error.JaxRsResourceGenerationException;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.grpc.Channel;
import io.grpc.stub.AbstractStub;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Path;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

@Slf4j
class ConfigServiceRestResourceConfig extends ResourceConfig {
  @Inject
  ConfigServiceRestResourceConfig(
      GuiceAdapterBinder binder,
      GrpcChannelRegistry grpcChannelRegistry,
      @Named("selfChannel") Channel selfChannel,
      TraceableConfigServiceRestConfig restConfig)
      throws JaxRsResourceGenerationException, GrpcStubGenerationException {
    registerGeneratedResources(grpcChannelRegistry, selfChannel, restConfig, binder);
    register(binder);
    packages("ai.traceable.config.service.rest");

    // list the endpoints / resources
    EndpointLister.listEndpoints(this);
  }

  private void registerGeneratedResources(
      GrpcChannelRegistry grpcChannelRegistry,
      Channel selfChannel,
      TraceableConfigServiceRestConfig restConfig,
      GuiceAdapterBinder binder)
      throws GrpcStubGenerationException, JaxRsResourceGenerationException {
    GrpcApiSpecsConfig grpcApiSpecsConfig = restConfig.getGrpcApiSpecsConfig();
    if (grpcApiSpecsConfig == null) {
      return;
    }
    JaxRsResourceGenerator resourceGenerator =
        JaxRsResourceGenerator.newBuilder(this.getClassLoader(), binder.guiceInjector)
            // save the generated resources
            .saveClasses()
            .build();
    var stubs = generateStubs(grpcApiSpecsConfig, grpcChannelRegistry, selfChannel);
    var resources = resourceGenerator.generateResources(stubs);
    for (var resource : resources) {
      if (resource.getClass().isAnnotationPresent(Path.class)) {
        register(resource.getClass());
        log.info("Registered generated resource: {}", resource.getClass().getName());
      } else {
        log.warn(
            "Generated resource does NOT have Path annotation, not registering: {}",
            resource.getClass().getName());
      }
    }
  }

  List<AbstractStub<?>> generateStubs(
      GrpcApiSpecsConfig grpcApiSpecsConfig,
      GrpcChannelRegistry grpcChannelRegistry,
      Channel selfChannel)
      throws GrpcStubGenerationException {
    List<AbstractStub<?>> stubs = new ArrayList<>();
    for (ChannelAndApiSpecsConfig config : grpcApiSpecsConfig.getChannelAndApiSpecsConfigs()) {
      var channelConfig = config.getChannelConfig();
      var host = channelConfig.getHost();
      Channel channel = null;
      if (host.equalsIgnoreCase("inprocess")) {
        channel = selfChannel;
      } else {
        channel = grpcChannelRegistry.forPlaintextAddress(host, channelConfig.getPort());
      }
      for (String grpcClassName : config.getGrpcClassNames()) {
        var stub = GrpcStubGenerator.generateBlockingStub(grpcClassName, channel);
        stubs.add(stub);
      }
    }
    return stubs;
  }

  @AllArgsConstructor(onConstructor_ = @Inject)
  private static class GuiceAdapterBinder extends AbstractBinder {
    private final Injector guiceInjector;

    @Override
    protected void configure() {
      guiceInjector.getAllBindings().keySet().forEach(this::bindKey);
    }

    protected <T> void bindKey(Key<T> key) {
      bindFactory(new InstanceFactory<>(key)).to(key.getTypeLiteral().getType());
    }

    @AllArgsConstructor
    private class InstanceFactory<T> implements Factory<T> {
      private final Key<T> key;

      @Override
      public T provide() {
        return guiceInjector.getInstance(key);
      }

      @Override
      public void dispose(T instance) {}
    }
  }
}
