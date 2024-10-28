package ai.traceable.config.service.rest.codegen;

import ai.traceable.config.service.rest.codegen.error.GrpcStubGenerationException;
import ai.traceable.config.service.rest.codegen.model.GrpcApiSpec;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.stub.AbstractBlockingStub;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class GrpcStubGenerator {
  public static AbstractBlockingStub<?> generateBlockingStub(
      GrpcApiSpec apiSpec, GrpcChannelRegistry grpcChannelRegistry)
      throws GrpcStubGenerationException {
    try {
      String className = apiSpec.getGrpcClassName();
      String host = apiSpec.getHost();
      int port = apiSpec.getPort();
      var channel = grpcChannelRegistry.forPlaintextAddress(host, port);
      return generateBlockingStub(className, channel);
    } catch (Throwable t) {
      throw new GrpcStubGenerationException(t);
    }
  }

  public static AbstractBlockingStub<?> generateBlockingStub(String grpcClassName, Channel channel)
      throws GrpcStubGenerationException {
    try {
      Class<?> klass = Class.forName(grpcClassName);
      AbstractBlockingStub<?> stub =
          (AbstractBlockingStub<?>)
              klass.getMethod("newBlockingStub", Channel.class).invoke(null, channel);
      stub =
          (AbstractBlockingStub<?>)
              stub.getClass()
                  .getMethod("withCallCredentials", CallCredentials.class)
                  .invoke(
                      stub,
                      RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider()
                          .get());
      return stub;
    } catch (Throwable t) {
      throw new GrpcStubGenerationException(t);
    }
  }
}
