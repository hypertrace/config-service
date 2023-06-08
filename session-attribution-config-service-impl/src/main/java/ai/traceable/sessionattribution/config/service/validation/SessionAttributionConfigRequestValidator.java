package ai.traceable.sessionattribution.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.DeleteSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRuleScope;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleRequest;
import io.grpc.Status;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SessionAttributionConfigRequestValidator {
  private final SessionTokenRuleValidator tokenRuleValidator;

  public void validateGetRequest(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateCreateRequest(
      RequestContext requestContext, CreateSessionAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateSessionAttributionRuleRequest.NAME_FIELD_NUMBER);
    tokenRuleValidator.validateTokenRules(request.getTokenRulesList());
    if (request.hasScope()) {
      validateRuleScope(request.getScope());
    }
  }

  public void validateUpdateRequest(
      RequestContext requestContext, UpdateSessionAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSessionAttributionRuleRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateSessionAttributionRuleRequest.NAME_FIELD_NUMBER);
    tokenRuleValidator.validateTokenRules(request.getTokenRulesList());
    if (request.hasScope()) {
      validateRuleScope(request.getScope());
    }
  }

  public void validateDeleteRequest(
      RequestContext requestContext, DeleteSessionAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSessionAttributionRuleRequest.ID_FIELD_NUMBER);
  }

  private void validateRuleScope(SessionAttributionRuleScope scope) {
    if (scope.getEnvironmentNamesCount() == 0
        && scope.getServiceNameRegexesCount() == 0
        && scope.getUrlMatchRegexesCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("One of the scope must be non empty")
          .asRuntimeException();
    }
    validateNoDuplicates(scope.getEnvironmentNamesList());
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
}
