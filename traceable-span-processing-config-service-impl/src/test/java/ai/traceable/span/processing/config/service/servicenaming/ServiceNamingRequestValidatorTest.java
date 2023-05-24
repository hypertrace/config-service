package ai.traceable.span.processing.config.service.servicenaming;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction.RegexCaptureGroupNameAssignment;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction.StaticNameAssignment;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition.AttributeCondition;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition.ConditionMatchOperator;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleScope;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleScope.EnvironmentScope;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class ServiceNamingRequestValidatorTest {
  private static final CreateServiceNamingRuleRequest VALID_STATIC_CREATE_REQUEST =
      CreateServiceNamingRuleRequest.newBuilder()
          .setName("naming rule")
          .setDescription("rule description")
          .setScope(
              ServiceNamingRuleScope.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().addEnvironmentNames("first-env")))
          .setEnabled(true)
          .addConditions(
              ServiceNamingRuleCondition.newBuilder()
                  .setAttributeCondition(
                      AttributeCondition.newBuilder()
                          .setAttributeKey("attr-key")
                          .setOperator(ConditionMatchOperator.CONDITION_MATCH_OPERATOR_EQUALS)
                          .setValue("val")))
          .setAction(
              ServiceNamingRuleAction.newBuilder()
                  .setStaticNameAssignment(
                      StaticNameAssignment.newBuilder().setServiceName("custom-name")))
          .build();
  private static final UpdateServiceNamingRuleRequest VALID_STATIC_UPDATE_REQUEST =
      UpdateServiceNamingRuleRequest.newBuilder()
          .setId("id")
          .setName("naming rule")
          .setDescription("rule description")
          .setScope(
              ServiceNamingRuleScope.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().addEnvironmentNames("first-env")))
          .setEnabled(true)
          .addConditions(
              ServiceNamingRuleCondition.newBuilder()
                  .setAttributeCondition(
                      AttributeCondition.newBuilder()
                          .setAttributeKey("attr-key")
                          .setOperator(ConditionMatchOperator.CONDITION_MATCH_OPERATOR_EQUALS)
                          .setValue("val")))
          .setAction(
              ServiceNamingRuleAction.newBuilder()
                  .setStaticNameAssignment(
                      StaticNameAssignment.newBuilder().setServiceName("custom-name")))
          .build();

  private static final CreateServiceNamingRuleRequest VALID_DYNAMIC_CREATE_REQUEST =
      VALID_STATIC_CREATE_REQUEST.toBuilder()
          .setAction(
              ServiceNamingRuleAction.newBuilder()
                  .setRegexCaptureGroupNameAssignment(
                      RegexCaptureGroupNameAssignment.newBuilder()
                          .setAttributeKey("some.key")
                          .setRegexCaptureGroup("regex-(.*)")))
          .build();
  private static final UpdateServiceNamingRuleRequest VALID_DYNAMIC_UPDATE_REQUEST =
      VALID_STATIC_UPDATE_REQUEST.toBuilder()
          .setAction(
              ServiceNamingRuleAction.newBuilder()
                  .setRegexCaptureGroupNameAssignment(
                      RegexCaptureGroupNameAssignment.newBuilder()
                          .setAttributeKey("some.key")
                          .setRegexCaptureGroup("regex-(.*)")))
          .build();
  private static final RequestContext VALID_REQUEST_CONTEXT = RequestContext.forTenantId("test");

  ServiceNamingRequestValidator validator = new ServiceNamingRequestValidator();

  @Test
  void acceptsValidRequests() {
    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT);

    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_STATIC_CREATE_REQUEST);
    this.validator.validateOrThrow(
        VALID_REQUEST_CONTEXT,
        VALID_STATIC_CREATE_REQUEST.toBuilder().clearScope().clearDescription().build());

    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_STATIC_UPDATE_REQUEST);
    this.validator.validateOrThrow(
        VALID_REQUEST_CONTEXT,
        VALID_STATIC_UPDATE_REQUEST.toBuilder().clearScope().clearDescription().build());

    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_DYNAMIC_CREATE_REQUEST);
    this.validator.validateOrThrow(VALID_REQUEST_CONTEXT, VALID_DYNAMIC_UPDATE_REQUEST);
  }

  @Test
  void rejectsMissingContext() {
    assertInvalidArg(() -> this.validator.validateOrThrow(new RequestContext()));
    assertInvalidArg(
        () -> this.validator.validateOrThrow(new RequestContext(), VALID_STATIC_UPDATE_REQUEST));
    assertInvalidArg(
        () -> this.validator.validateOrThrow(new RequestContext(), VALID_STATIC_CREATE_REQUEST));
  }

  @Test
  void rejectsBadCreateRequest() {
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_CREATE_REQUEST.toBuilder().clearName().build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_CREATE_REQUEST.toBuilder().clearAction().build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_CREATE_REQUEST.toBuilder()
                    .setAction(ServiceNamingRuleAction.newBuilder()) // missing name
                    .build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_CREATE_REQUEST.toBuilder()
                    .clearConditions()
                    .addConditions(
                        ServiceNamingRuleCondition.newBuilder()
                            .setAttributeCondition( // missing operator
                                AttributeCondition.newBuilder().setAttributeKey("test")))
                    .build()));
  }

  @Test
  void rejectsBadUpdateRequest() {
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT, VALID_STATIC_UPDATE_REQUEST.toBuilder().clearId().build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_UPDATE_REQUEST.toBuilder().clearName().build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_UPDATE_REQUEST.toBuilder().clearAction().build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_UPDATE_REQUEST.toBuilder()
                    .setAction(ServiceNamingRuleAction.newBuilder()) // missing name
                    .build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_UPDATE_REQUEST.toBuilder().clearConditions().build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_STATIC_UPDATE_REQUEST.toBuilder()
                    .clearConditions()
                    .addConditions(
                        ServiceNamingRuleCondition.newBuilder()
                            .setAttributeCondition( // missing operator
                                AttributeCondition.newBuilder().setAttributeKey("test")))
                    .build()));
  }

  @Test
  void rejectsBadDynamicCreate() {
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_DYNAMIC_CREATE_REQUEST.toBuilder()
                    .setAction(
                        ServiceNamingRuleAction.newBuilder()
                            .setRegexCaptureGroupNameAssignment(
                                RegexCaptureGroupNameAssignment.newBuilder()
                                    .setAttributeKey("key") // missing regex
                                    .build()))
                    .build()));
    assertInvalidArg(
        () ->
            this.validator.validateOrThrow(
                VALID_REQUEST_CONTEXT,
                VALID_DYNAMIC_CREATE_REQUEST.toBuilder()
                    .setAction(
                        ServiceNamingRuleAction.newBuilder()
                            .setRegexCaptureGroupNameAssignment(
                                RegexCaptureGroupNameAssignment.newBuilder()
                                    .setAttributeKey("key")
                                    .setRegexCaptureGroup("regex-without-capture")
                                    .build()))
                    .build()));
  }

  void assertInvalidArg(Executable executable) {
    assertEquals(
        Code.INVALID_ARGUMENT,
        Assertions.assertThrows(StatusRuntimeException.class, executable).getStatus().getCode());
  }
}
