package ai.traceable.jwt.extraction.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.jwt.extraction.config.service.v1.CreateJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.DeleteJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import ai.traceable.jwt.extraction.config.service.v1.PathPredicate;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import ai.traceable.jwt.extraction.config.service.v1.Predicate.CompositePredicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate.RelationalOperator;
import ai.traceable.jwt.extraction.config.service.v1.UpdateJwtExtractionRuleRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
class JwtExtractionConfigRequestValidator {
  private final JwtExtractionRuleManager ruleManager;

  void validateOrThrow(RequestContext requestContext, GetJwtExtractionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  void validateOrThrow(RequestContext requestContext, UpdateJwtExtractionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateJwtExtractionRuleRequest.ID_FIELD_NUMBER);
    validatePredicate(request.getPredicate());
    if (request.getLocationsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Locations list cannot be empty")
          .asRuntimeException();
    }
    request.getLocationsList().forEach(this::validateJwtLocation);
    if (request.getInstructionsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Instructions list cannot be empty")
          .asRuntimeException();
    }
    request.getInstructionsList().forEach(this::validateJwtProcessingInstruction);

    if (!ruleManager.ruleExists(requestContext, request.getId())) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Unable to find requested rule for update in context %s for request %s",
                  requestContext, request))
          .asRuntimeException();
    }
  }

  void validateOrThrow(RequestContext requestContext, CreateJwtExtractionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validatePredicate(request.getPredicate());
    if (request.getLocationsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Locations list cannot be empty")
          .asRuntimeException();
    }
    request.getLocationsList().forEach(this::validateJwtLocation);
    request.getInstructionsList().forEach(this::validateJwtProcessingInstruction);
    if (request.getInstructionsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Instructions list cannot be empty")
          .asRuntimeException();
    }
  }

  void validateOrThrow(RequestContext requestContext, DeleteJwtExtractionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteJwtExtractionRuleRequest.ID_FIELD_NUMBER);
  }

  void validatePredicate(Predicate predicate) {
    switch (predicate.getPredicateCase()) {
      case COMPOSITE_PREDICATE:
        validateCompositePredicate(predicate.getCompositePredicate());
        break;
      case URL_PREDICATE:
        validateStringPredicate(predicate.getUrlPredicate());
        break;
      case PREDICATE_NOT_SET:
      default:
        // Valid
    }
  }

  void validateJwtProcessingInstruction(JwtProcessingInstruction instruction) {
    validateValueExtraction(instruction.getValueExtraction());
    validateAction(instruction.getAction());
  }

  void validateValueExtraction(JwtProcessingInstruction.ValueExtraction valueExtraction) {
    if (valueExtraction.getSourceCase()
        == JwtProcessingInstruction.ValueExtraction.SourceCase.SOURCE_NOT_SET) {
      throw Status.INVALID_ARGUMENT
          .withDescription("JwtDataLocation has no source case set")
          .asRuntimeException();
    }
    if (valueExtraction.getCaptureCase()
        == JwtProcessingInstruction.ValueExtraction.CaptureCase.CAPTURE_NOT_SET) {
      throw Status.INVALID_ARGUMENT
          .withDescription("JwtDataLocation has no capture case set")
          .asRuntimeException();
    }
  }

  void validateAction(JwtProcessingInstruction.Action action) {
    if (action.getActionCase() == JwtProcessingInstruction.Action.ActionCase.ACTION_NOT_SET) {
      throw Status.INVALID_ARGUMENT
          .withDescription("JwtAction has no action case set")
          .asRuntimeException();
    }
  }

  void validateJwtLocation(JwtLocation location) {
    if (location.hasRegexCaptureGroup()) {
      Status status = RegexValidator.validateRegex(location.getRegexCaptureGroup());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
    switch (location.getLocationCase()) {
      case REQUEST_HEADER:
        validateStringPredicateJwtLocation(location.getRequestHeader());
        break;
      case REQUEST_COOKIE:
        validateStringPredicateJwtLocation(location.getRequestCookie());
        break;
      case QUERY_PARAMETER:
        validateStringPredicateJwtLocation(location.getQueryParameter());
        break;
      case FORM_REQUEST_BODY:
        validateStringPredicateJwtLocation(location.getFormRequestBody());
        break;
      case JSON_REQUEST_BODY:
        validatePathPredicateJwtLocation(location.getJsonRequestBody());
        break;
      case LOCATION_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Missing Location").asRuntimeException();
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
    if (compositePredicate.getChildrenCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Composite predicate children cannot be empty")
          .asRuntimeException();
    }
    compositePredicate.getChildrenList().forEach(this::validatePredicate);
  }

  void validateStringPredicate(StringPredicate stringPredicate) {
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.VALUE_FIELD_NUMBER);
    if (stringPredicate.getOperator() == RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX
        || stringPredicate.getOperator()
            == RelationalOperator.RELATIONAL_OPERATOR_NOT_MATCHES_REGEX) {
      Status status = RegexValidator.validateRegex(stringPredicate.getValue());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }

  void validatePathPredicateJwtLocation(PathPredicate pathPredicate) {
    switch (pathPredicate.getPathCase()) {
      case KEY_PREDICATE:
        validateStringPredicateJwtLocation(pathPredicate.getKeyPredicate());
        break;
      case PATH_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("PathPredicate has no path case set")
            .asRuntimeException();
    }
  }

  void validateStringPredicateJwtLocation(StringPredicate stringPredicate) {
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.OPERATOR_FIELD_NUMBER);
    if (stringPredicate.getOperator() != RelationalOperator.RELATIONAL_OPERATOR_EQUALS) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Only EQUALS relational operator supported under JwtLocation")
          .asRuntimeException();
    }
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.VALUE_FIELD_NUMBER);
  }
}
