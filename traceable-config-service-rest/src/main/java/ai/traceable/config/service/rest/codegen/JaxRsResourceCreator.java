package ai.traceable.config.service.rest.codegen;

import ai.traceable.config.service.rest.codegen.error.JaxRsResourceGenerationException;
import io.grpc.stub.AbstractStub;

/** This class is responsible for generation of a JAX-RS resource, given the rpc definition. */
public interface JaxRsResourceCreator {
  /**
   * Generate a JAX-RS resource for the given parameters.
   *
   * @param classLoader in which the instance of the generated resource is bound. Callers are
   *     responsible for providing appropriate classLoader, typically from the container context so
   *     that the resources are available for binding.
   * @param clazz definition of the Grpc
   * @param clientStub an instance of the stub, that can communicate with an implementation of the
   *     provided Grpc
   * @return JAX-RS resource
   * @throws JaxRsResourceGenerationException if the class is not found, or stub is not provided
   */
  Object createJaxRsResource(
      ClassLoader classLoader, Class<? extends AbstractStub> clazz, AbstractStub<?> clientStub)
      throws JaxRsResourceGenerationException;
}
