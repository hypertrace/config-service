package ai.traceable.agent.action.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.agent.action.config.service.v1.AgentActionDetails;
import ai.traceable.agent.action.config.service.v1.AgentActionInput;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.DeleteAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsFilter;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsRequest;
import ai.traceable.agent.action.config.service.v1.RestartAgentAction;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionRequest;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AgentActionRequestValidatorTest {

  private AgentActionRequestValidator validator;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    validator = new AgentActionRequestValidator();
    requestContext = RequestContext.forTenantId("test-tenant");
  }

  @Test
  void validateCreateAgentAction_ValidRequest_ShouldNotThrow() {
    CreateAgentActionRequest request =
        CreateAgentActionRequest.newBuilder().setAction(createValidAgentActionInput()).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateCreateAgentAction_MissingAction_ShouldThrow() {
    CreateAgentActionRequest request = CreateAgentActionRequest.getDefaultInstance();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    assertEquals(
        "Missing agent action inside create request", exception.getStatus().getDescription());
  }

  @Test
  void validateCreateAgentAction_NoActionDetails_ShouldThrow() {
    AgentActionInput emptyInput = AgentActionInput.getDefaultInstance();
    CreateAgentActionRequest request =
        CreateAgentActionRequest.newBuilder().setAction(emptyInput).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    assertEquals(
        "No actions provided in agent action input", exception.getStatus().getDescription());
  }

  @Test
  void validateGetAgentActions_NoFilter_ShouldThrow() {
    GetAgentActionsRequest request = GetAgentActionsRequest.getDefaultInstance();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateGetAgentActions_validRequest_ShouldNotThrow() {
    GetAgentActionsRequest request =
        GetAgentActionsRequest.newBuilder().setFilter(GetAgentActionsFilter.newBuilder()).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateUpdateAgentAction_validRequest_ShouldNotThrow() {
    UpdateAgentActionRequest request =
        UpdateAgentActionRequest.newBuilder()
            .setId("test-id")
            .setAction(createValidAgentActionInput())
            .build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateUpdateAgentAction_MissingId_ShouldThrow() {
    UpdateAgentActionRequest request =
        UpdateAgentActionRequest.newBuilder().setAction(createValidAgentActionInput()).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  @Test
  void validateUpdateAgentAction_NoActionDetails_ShouldThrow() {
    AgentActionInput emptyInput = AgentActionInput.getDefaultInstance();
    UpdateAgentActionRequest request =
        UpdateAgentActionRequest.newBuilder().setId("test-id").setAction(emptyInput).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    assertEquals(
        "No actions provided in agent action input", exception.getStatus().getDescription());
  }

  @Test
  void validateDeleteAgentAction_validRequest_ShouldNotThrow() {
    DeleteAgentActionRequest request =
        DeleteAgentActionRequest.newBuilder().setId("test-id").build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateDeleteAgentAction_MissingId_ShouldThrow() {
    DeleteAgentActionRequest request = DeleteAgentActionRequest.getDefaultInstance();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  @Test
  void validateAgentActionInput_NoActionDetail_ShouldThrow() {
    AgentActionInput input =
        AgentActionInput.newBuilder().addDetails(AgentActionDetails.getDefaultInstance()).build();
    CreateAgentActionRequest request =
        CreateAgentActionRequest.newBuilder().setAction(input).build();
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    assertEquals(
        "No action detail provided in agent action input", exception.getStatus().getDescription());
  }

  private AgentActionInput createValidAgentActionInput() {
    AgentActionDetails details =
        AgentActionDetails.newBuilder()
            .setRestartAgentAction(RestartAgentAction.getDefaultInstance())
            .build();

    return AgentActionInput.newBuilder().addAllDetails(ImmutableList.of(details)).build();
  }
}
