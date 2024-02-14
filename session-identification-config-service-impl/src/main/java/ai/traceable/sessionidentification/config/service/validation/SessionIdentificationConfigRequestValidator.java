package ai.traceable.sessionidentification.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.sessionidentification.config.service.v1.CreateSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.DeleteSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleScope;
import ai.traceable.sessionidentification.config.service.v1.UpdateSessionIdentificationRuleRequest;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
@Slf4j
public class SessionIdentificationConfigRequestValidator {
  private final SessionTokenRuleValidator tokenRuleValidator;

  public void validateGetRequest(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateSessionIdentificationRule(SessionIdentificationRule rule) {
    validateNonDefaultPresenceOrThrow(rule, SessionIdentificationRule.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(rule, SessionIdentificationRule.NAME_FIELD_NUMBER);
    tokenRuleValidator.validateTokenRules(rule.getTokenRulesList());
    if (rule.hasScope()) {
      validateRuleScope(rule.getScope());
    }
  }

  public void validateCreateRequest(
      RequestContext requestContext,
      CreateSessionIdentificationRuleRequest request,
      List<SessionIdentificationRule> existingSessionIdentificationRules) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateSessionIdentificationRuleRequest.NAME_FIELD_NUMBER);
    validateRuleForSameName(request.getName(), existingSessionIdentificationRules);
    tokenRuleValidator.validateTokenRules(request.getTokenRulesList());
    if (request.hasScope()) {
      validateRuleScope(request.getScope());
    }
  }

  public void validateUpdateRequest(
      RequestContext requestContext,
      UpdateSessionIdentificationRuleRequest request,
      List<SessionIdentificationRule> existingSessionIdentificationRules) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateSessionIdentificationRuleRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateSessionIdentificationRuleRequest.NAME_FIELD_NUMBER);
    validateRuleForSameName(request.getName(), existingSessionIdentificationRules);
    tokenRuleValidator.validateTokenRules(request.getTokenRulesList());
    if (request.hasScope()) {
      validateRuleScope(request.getScope());
    }
  }

  public void validateDeleteRequest(
      RequestContext requestContext, DeleteSessionIdentificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteSessionIdentificationRuleRequest.ID_FIELD_NUMBER);
  }

  private void validateRuleScope(SessionIdentificationRuleScope scope) {
    if (scope.getEnvironmentNamesCount() == 0
        && scope.getServiceNameRegexesCount() == 0
        && scope.getUrlMatchRegexesCount() == 0
        && scope.getServiceNamesCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("One of the scope must be non empty")
          .asRuntimeException();
    }
    validateNoDuplicates(scope.getEnvironmentNamesList());
    validateNoDuplicates(scope.getServiceNamesList());
    validateRegexList(scope.getServiceNameRegexesList());
    validateRegexList(scope.getUrlMatchRegexesList());
  }

  private void validateRegexList(List<String> regexes) {
    regexes.forEach(
        regex -> {
          Status status = RegexValidator.validate(regex);
          if (!status.isOk()) {
            throw status.asRuntimeException();
          }
        });

    validateNoDuplicates(regexes);
  }

  private void validateNoDuplicates(List<String> scopes) {
    try {
      // validates there are no duplicates present
      Set.of(scopes.toArray());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
    }
  }

  private void validateRuleForSameName(String ruleName, List<SessionIdentificationRule> rules) {
    Optional<SessionIdentificationRule> existingRuleWithSameName =
        rules.stream().filter(rule -> rule.getName().equals(ruleName)).findFirst();
    if (existingRuleWithSameName.isPresent()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Session Identification rule with name %s already exists", ruleName))
          .asRuntimeException();
    }
  }
}
