package ai.traceable.config.service.rest.codegen;

import ai.traceable.config.service.rest.codegen.error.JaxRsResourceGenerationException;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import com.google.inject.Provider;
import javax.ws.rs.Path;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class DefaultJaxRsResourceCreatorTest {
  private GrpcChannelRegistry channelRegistry;
  private JaxRsResourceCreator target;
  @Mock Provider<RequestContext> requestContextProvider;

  @BeforeEach
  public void setup() {
    this.channelRegistry = new GrpcChannelRegistry();
    this.target = new DefaultJaxRsResourceCreator(requestContextProvider);
  }

  @Test
  public void test() throws JaxRsResourceGenerationException {
    Object resource =
        target.createJaxRsResource(
            getClass().getClassLoader(),
            FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub.class,
            FraudPolicyConfigServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress("localhost", 8080)));
    Assertions.assertTrue(resource.getClass().isAnnotationPresent(Path.class));
  }

  @Test
  public void testFraudPolicy() throws JaxRsResourceGenerationException {
    Object resource =
        target.createJaxRsResource(
            getClass().getClassLoader(),
            FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub.class,
            FraudPolicyConfigServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress("localhost", 8080)));
    Assertions.assertTrue(resource.getClass().isAnnotationPresent(Path.class));
  }
}
