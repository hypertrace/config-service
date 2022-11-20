package ai.traceable.authorization.detection.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.authorization.detection.config.service.v1.CreateAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.DeleteAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.GetAuthorizationDetectionRulesRequest;
import ai.traceable.authorization.detection.config.service.v1.Predicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.CompositePredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.KeyValuePredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.PathPredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.PathValuePredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.StringPredicate;
import ai.traceable.authorization.detection.config.service.v1.UpdateAuthorizationDetectionRuleRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
class AuthorizationDetectionConfigRequestValidator {
  private final AuthorizationDetectionRuleStore ruleStore;

  void validateOrThrow(
      RequestContext requestContext, GetAuthorizationDetectionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  void validateOrThrow(
      RequestContext requestContext, UpdateAuthorizationDetectionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateAuthorizationDetectionRuleRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateAuthorizationDetectionRuleRequest.AUTHORIZATION_TYPE_FIELD_NUMBER);
    validatePredicate(request.getPredicate());

    ruleStore
        .getObject(requestContext, request.getId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        String.format(
                            "Unable to find requested rule for update in context %s for request %s",
                            requestContext, request))
                    .asRuntimeException());
  }

  void validateOrThrow(
      RequestContext requestContext, CreateAuthorizationDetectionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateAuthorizationDetectionRuleRequest.AUTHORIZATION_TYPE_FIELD_NUMBER);
    validatePredicate(request.getPredicate());
  }

  void validateOrThrow(
      RequestContext requestContext, DeleteAuthorizationDetectionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteAuthorizationDetectionRuleRequest.ID_FIELD_NUMBER);
  }

  void validatePredicate(Predicate predicate) {
    switch (predicate.getPredicateCase()) {
      case COMPOSITE_PREDICATE:
        validateCompositePredicate(predicate.getCompositePredicate());
        break;
      case HEADER_PREDICATE:
        validateKeyValuePredicate(predicate.getHeaderPredicate());
        break;
      case COOKIE_PREDICATE:
        validateKeyValuePredicate(predicate.getCookiePredicate());
        break;
      case QUERY_PARAMETER_PREDICATE:
        validateKeyValuePredicate(predicate.getQueryParameterPredicate());
        break;
      case FORM_BODY_PREDICATE:
        validateKeyValuePredicate(predicate.getFormBodyPredicate());
        break;
      case JSON_BODY_PREDICATE:
        validatePathValuePredicate(predicate.getJsonBodyPredicate());
        break;
      case PREDICATE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Missing Predicate").asRuntimeException();
    }
  }

  void validateCompositePredicate(CompositePredicate compositePredicate) {
    switch (compositePredicate.getOperator()) {
      case UNRECOGNIZED:
      case LOGICAL_OPERATOR_UNSPECIFIED:
        throw Status.INVALID_ARGUMENT.withDescription("Missing Predicate").asRuntimeException();
      case LOGICAL_OPERATOR_OR:
      case LOGICAL_OPERATOR_AND:
      default:
        // Valid
    }
    compositePredicate.getChildrenList().forEach(this::validatePredicate);
  }

  void validatePathValuePredicate(PathValuePredicate pathValuePredicate) {
    // You must have both a path and value predicate for a path value predicate
    this.validatePathPredicate(pathValuePredicate.getPathPredicate());
    this.validateStringPredicate(pathValuePredicate.getValuePredicate());
  }

  void validateKeyValuePredicate(KeyValuePredicate keyValuePredicate) {
    // We can have either key predicate, value predicate or both. At least one must be set.
    if (!keyValuePredicate.hasKeyPredicate() && !keyValuePredicate.hasValuePredicate()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("KeyValuePredicate has neither key nor value predicate set")
          .asRuntimeException();
    }
    if (keyValuePredicate.hasKeyPredicate()) {
      this.validateStringPredicate(keyValuePredicate.getKeyPredicate());
    }
    if (keyValuePredicate.hasValuePredicate()) {
      this.validateStringPredicate(keyValuePredicate.getValuePredicate());
    }
  }

  void validatePathPredicate(PathPredicate pathPredicate) {
    switch (pathPredicate.getPathCase()) {
      case KEY_PREDICATE:
        validateStringPredicate(pathPredicate.getKeyPredicate());
        break;
      case PATH_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("PathPredicate has no path case set")
            .asRuntimeException();
    }
  }

  void validateStringPredicate(StringPredicate stringPredicate) {
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.VALUE_FIELD_NUMBER);
  }
}
