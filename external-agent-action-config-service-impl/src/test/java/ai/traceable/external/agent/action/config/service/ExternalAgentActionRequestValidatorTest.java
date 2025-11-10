package ai.traceable.external.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.external.agent.action.config.service.v1.AgentScope;
import ai.traceable.external.agent.action.config.service.v1.GetAgentActionsRequest;
import ai.traceable.external.agent.action.config.service.v1.ScopedActionRequest;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExternalAgentActionRequestValidatorTest {

  private ExternalAgentActionRequestValidator validator;
  private RequestContext mockRequestContext;
  private GetAgentActionsRequest mockRequest;

  @BeforeEach
  void setUp() {
    validator = new ExternalAgentActionRequestValidator();
    mockRequestContext = RequestContext.forTenantId("test-tenant");
    mockRequest =
        GetAgentActionsRequest.newBuilder()
            .addAgentActionRequests(
                ScopedActionRequest.newBuilder()
                    .setScope(
                        AgentScope.newBuilder()
                            .setDeploymentName("test-deployment")
                            .setServiceInstanceId("test-service-instance")
                            .setModuleVersion("test-module-version")
                            .setModuleName("test-module-name")))
            .build();
  }

  @Test
  void validateOrThrow_shouldNotThrowForValidRequest() {
    assertDoesNotThrow(() -> validator.validateOrThrow(mockRequestContext, mockRequest));
  }

  @Test
  void validateOrThrow_shouldThrowWhenRequestContextIsNull() {
    assertThrows(
        NullPointerException.class,
        () -> validator.validateOrThrow(null, mockRequest),
        "Request context cannot be null");
  }

  @Test
  void validateOrThrow_shouldThrowWhenRequestIsNull() {
    assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetAgentActionsRequest.newBuilder().build()),
        "Request cannot be null");
  }
}
