package ai.traceable.external.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionConfigServiceGrpc;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRules;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExternalUserAttributionConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-id";
  private static final UserAttributionRule USER_ATTRIBUTION_RULE_1 =
      UserAttributionRule.newBuilder()
          .setData(
              UserAttributionRuleData.newBuilder()
                  .setCustomData(
                      UserAttributionRuleData.CustomUserAttributionRuleData.newBuilder()
                          .setYaml("rule-yaml1")))
          .setScope(
              UserAttributionRuleScope.newBuilder()
                  .setCustomScope(
                      UserAttributionRuleScope.CustomScope.newBuilder()
                          .addEnvironmentScopes(
                              UserAttributionRuleScope.EnvironmentScope.newBuilder()
                                  .setEnvironmentName("env1")
                                  .build())
                          .build())
                  .build())
          .build();
  private static final UserAttributionRule USER_ATTRIBUTION_RULE_2 =
      UserAttributionRule.newBuilder()
          .setData(
              UserAttributionRuleData.newBuilder()
                  .setCustomData(
                      UserAttributionRuleData.CustomUserAttributionRuleData.newBuilder()
                          .setYaml("rule-yaml2")))
          .setScope(
              UserAttributionRuleScope.newBuilder()
                  .setCustomScope(
                      UserAttributionRuleScope.CustomScope.newBuilder()
                          .addEnvironmentScopes(
                              UserAttributionRuleScope.EnvironmentScope.newBuilder()
                                  .setEnvironmentName("env2")
                                  .build())
                          .build())
                  .build())
          .build();

  private ExternalUserAttributionConfigServiceGrpc.ExternalUserAttributionConfigServiceBlockingStub
      externalUserAttributionStub;
  private ExternalUserAttributionRuleTranslator externalUserAttributionRuleTranslator;
  private Server mockServer;

  @BeforeEach
  void beforeEach() throws IOException {
    externalUserAttributionRuleTranslator = new ExternalUserAttributionRuleTranslator();

    String uniqueName = InProcessServerBuilder.generateName();
    ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();

    UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub userAttributionStub =
        UserAttributionConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.externalUserAttributionStub =
        ExternalUserAttributionConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    mockServer =
        InProcessServerBuilder.forName(uniqueName)
            .directExecutor()
            .addService(new MockUserAttributionConfigService())
            .addService(
                ServerInterceptors.intercept(
                    new ExternalUserAttributionConfigServiceImpl(
                        userAttributionStub,
                        externalUserAttributionRuleTranslator,
                        new ExternalUserAttributionRuleResponseBuilder(new HashGenerator())),
                    new TestInterceptor()))
            .build()
            .start();
  }

  @AfterEach
  void afterEach() {
    this.mockServer.shutdown();
  }

  @Test
  void testGetExternalUserAttributionRulesWithEnvironment() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);

    ExternalUserAttributionRules externalUserAttributionRules =
        requestContext.call(
            () ->
                externalUserAttributionStub
                    .getExternalUserAttributionRules(
                        GetExternalUserAttributionRulesRequest.getDefaultInstance())
                    .getExternalUserAttributionRules());

    ExternalUserAttributionRules expectedExternalUserAttributionRules =
        externalUserAttributionRuleTranslator.translateRules(
            List.of(USER_ATTRIBUTION_RULE_1, USER_ATTRIBUTION_RULE_2));

    assertEquals(2, externalUserAttributionRules.getExternalUserAttributionRuleCount());
    assertEquals(expectedExternalUserAttributionRules, externalUserAttributionRules);

    externalUserAttributionRules =
        requestContext.call(
            () ->
                externalUserAttributionStub
                    .getExternalUserAttributionRules(
                        GetExternalUserAttributionRulesRequest.newBuilder()
                            .setEnvironmentName("env1")
                            .build())
                    .getExternalUserAttributionRules());

    expectedExternalUserAttributionRules =
        externalUserAttributionRuleTranslator.translateRules(List.of(USER_ATTRIBUTION_RULE_1));

    assertEquals(1, externalUserAttributionRules.getExternalUserAttributionRuleCount());
    assertEquals(expectedExternalUserAttributionRules, externalUserAttributionRules);

    externalUserAttributionRules =
        requestContext.call(
            () ->
                externalUserAttributionStub
                    .getExternalUserAttributionRules(
                        GetExternalUserAttributionRulesRequest.newBuilder()
                            .setEnvironmentName("env2")
                            .build())
                    .getExternalUserAttributionRules());

    expectedExternalUserAttributionRules =
        externalUserAttributionRuleTranslator.translateRules(List.of(USER_ATTRIBUTION_RULE_2));

    assertEquals(1, externalUserAttributionRules.getExternalUserAttributionRuleCount());
    assertEquals(expectedExternalUserAttributionRules, externalUserAttributionRules);
  }

  static class MockUserAttributionConfigService
      extends UserAttributionConfigServiceGrpc.UserAttributionConfigServiceImplBase {

    @Override
    public void getUserAttributionRules(
        GetUserAttributionRulesRequest request,
        StreamObserver<GetUserAttributionRulesResponse> responseObserver) {
      responseObserver.onNext(
          GetUserAttributionRulesResponse.newBuilder()
              .addAllRules(List.of(USER_ATTRIBUTION_RULE_1, USER_ATTRIBUTION_RULE_2))
              .build());
      responseObserver.onCompleted();
    }
  }

  private static class TestInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      Context ctx =
          Context.current()
              .withValue(
                  RequestContext.CURRENT,
                  RequestContext.forTenantId(
                      headers.get(
                          Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER))));
      return Contexts.interceptCall(ctx, call, headers, next);
    }
  }
}
