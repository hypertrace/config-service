package ai.traceable.config.service.rest.codegen;

import ai.traceable.config.service.rest.codegen.error.JaxRsResourceGenerationException;
import ai.traceable.config.service.rest.codegen.model.GrpcApiSpec;
import com.google.inject.Injector;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

/**
 * Entry point into this module. Instantiate the resource generator and provide a list of grpc api
 * specs, it will return the jax rs resources for them that can be injected into a rest container.
 */
public class JaxRsResourceGenerator {
  private final JaxRsResourceCreator jaxRsResourceCreator;
  private final ClassLoader classLoader;
  private final GrpcChannelRegistry grpcChannelRegistry;

  /**
   * @param jaxRsResourceCreator resource creator
   * @param classLoader Provide an appropriate classLoader (usually, the one being used to inject
   *     resources)
   * @param grpcChannelRegistry Provide the grpc channel registry to instantiate client stubs.
   */
  private JaxRsResourceGenerator(
      JaxRsResourceCreator jaxRsResourceCreator,
      ClassLoader classLoader,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.jaxRsResourceCreator = jaxRsResourceCreator;
    this.classLoader = classLoader;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  public static Builder newBuilder(ClassLoader classLoader, Injector injector) {
    return new Builder(classLoader, injector);
  }

  public static class Builder {
    private final JaxRsResourceCreator jaxRsResourceCreator;
    private final ClassLoader classLoader;
    private final GrpcChannelRegistry grpcChannelRegistry;

    Builder(ClassLoader classLoader, Injector injector) {
      this.classLoader = classLoader;
      this.jaxRsResourceCreator = injector.getInstance(JaxRsResourceCreator.class);
      this.grpcChannelRegistry = injector.getInstance(GrpcChannelRegistry.class);
    }

    public Builder saveClasses() {
      return this;
    }

    public JaxRsResourceGenerator build() {
      return new JaxRsResourceGenerator(jaxRsResourceCreator, classLoader, grpcChannelRegistry);
    }
  }

  public List<Object> generateResources(List<AbstractStub<?>> stubs)
      throws JaxRsResourceGenerationException {
    try {
      List<Object> jaxRsResources = new ArrayList<>();
      for (var stub : stubs) {
        var jaxRsResource =
            jaxRsResourceCreator.createJaxRsResource(classLoader, stub.getClass(), stub);
        jaxRsResources.add(jaxRsResource);
      }
      return jaxRsResources;
    } catch (Throwable t) {
      throw new JaxRsResourceGenerationException(t);
    }
  }

  /**
   * Given a set of apiSpecs, generate JAX-RS resources An interceptor is generated using the spec
   * itself
   */
  public List<Object> generateStubsAndResources(List<GrpcApiSpec> apiSpecs)
      throws JaxRsResourceGenerationException {
    try {
      List<AbstractStub<?>> stubs = new ArrayList<>();
      for (GrpcApiSpec apiSpec : apiSpecs) {
        AbstractBlockingStub<? extends AbstractBlockingStub<?>> stub =
            GrpcStubGenerator.generateBlockingStub(apiSpec, grpcChannelRegistry);
        stubs.add(stub);
      }
      return generateResources(stubs);
    } catch (Throwable t) {
      throw new JaxRsResourceGenerationException(t);
    }
  }
}
