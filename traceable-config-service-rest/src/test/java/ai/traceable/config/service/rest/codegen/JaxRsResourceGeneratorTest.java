package ai.traceable.config.service.rest.codegen;

import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrideServiceGrpc;
import ai.traceable.config.service.rest.codegen.error.JaxRsResourceGenerationException;
import ai.traceable.config.service.rest.codegen.model.GrpcApiSpec;
import ai.traceable.fraud.datamodel.config.service.v1.FraudDataModelConfigServiceGrpc;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.FraudDataModelDerivationConfigServiceGrpc;
import ai.traceable.fraud.engine.ml.FraudEngineMLServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.risk.decision.service.api.v1.RiskDecisionServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Provider;
import javax.ws.rs.Path;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JaxRsResourceGeneratorTest {
  private Injector injector;

  public static class TestModule extends AbstractModule {
    @Override
    public void configure() {
      Provider<RequestContext> requestContextProvider = () -> RequestContext.forTenantId("default");
      bind(GrpcChannelRegistry.class).toInstance(new GrpcChannelRegistry());
      bind(JaxRsResourceCreator.class)
          .toInstance(new DefaultJaxRsResourceCreator(requestContextProvider));
    }
  }

  @BeforeEach
  public void setup() {
    this.injector = Guice.createInjector(new TestModule());
  }

  @Test
  public void test() throws JaxRsResourceGenerationException {

    JaxRsResourceGenerator generator =
        JaxRsResourceGenerator.newBuilder(getClass().getClassLoader(), injector)
            .saveClasses("build")
            .build();

    GrpcApiSpec apiSpec =
        new GrpcApiSpec(FraudPolicyConfigServiceGrpc.class.getName(), "localhost", 8080);
    var output = generator.generateStubsAndResources(Collections.singletonList(apiSpec));
    System.out.println(output);
    Assertions.assertEquals(1, output.size());
    Assertions.assertTrue(output.get(0).getClass().isAnnotationPresent(Path.class));
  }

  @Test
  public void testMultiple() throws JaxRsResourceGenerationException {
    JaxRsResourceGenerator generator =
        JaxRsResourceGenerator.newBuilder(getClass().getClassLoader(), injector)
            .saveClasses("build")
            .build();

    List<GrpcApiSpec> apiSpecList = new ArrayList<>();
    apiSpecList.add(
        new GrpcApiSpec(AnomalyGlobalConfigServiceGrpc.class.getName(), "localhost", 8080));
    apiSpecList.add(
        new GrpcApiSpec(FraudPolicyConfigServiceGrpc.class.getName(), "localhost", 8080));
    apiSpecList.add(new GrpcApiSpec(RiskDecisionServiceGrpc.class.getName(), "localhost", 8080));
    apiSpecList.add(
        new GrpcApiSpec(ApiAttributeOverrideServiceGrpc.class.getName(), "localhost", 8080));
    apiSpecList.add(
        new GrpcApiSpec(FraudDataModelConfigServiceGrpc.class.getName(), "localhost", 8080));
    apiSpecList.add(
        new GrpcApiSpec(
            FraudDataModelDerivationConfigServiceGrpc.class.getName(), "localhost", 8080));
    apiSpecList.add(new GrpcApiSpec(FraudEngineMLServiceGrpc.class.getName(), "localhost", 8080));
    List<Object> resources = generator.generateStubsAndResources(apiSpecList);
    System.out.println(resources);
    Assertions.assertEquals(apiSpecList.size(), resources.size());
    for (Object resource : resources) {
      Assertions.assertTrue(resource.getClass().isAnnotationPresent(Path.class));
    }
  }
}
