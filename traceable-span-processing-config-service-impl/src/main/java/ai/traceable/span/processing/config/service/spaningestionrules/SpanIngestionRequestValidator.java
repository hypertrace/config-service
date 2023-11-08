package ai.traceable.span.processing.config.service.spaningestionrules;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigRequest;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRuleData;
import ai.traceable.span.processing.config.service.v1.Predicate;
import ai.traceable.span.processing.config.service.v1.RankSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.RetentionAction;
import ai.traceable.span.processing.config.service.v1.StringPredicate;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

class SpanIngestionRequestValidator {

  void validateOrThrow(RequestContext requestContext, GetSpanIngestionConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetSpanIngestionConfigRequest.STAGE_FIELD_NUMBER);
  }

  void validateOrThrow(RequestContext requestContext, CreateSpanIngestionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateRule(request);
  }

  void validateOrThrow(RequestContext requestContext, DeleteSpanIngestionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSpanIngestionRuleRequest.ID_FIELD_NUMBER);
  }

  void validateOrThrow(RequestContext requestContext, UpdateSpanIngestionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSpanIngestionRuleRequest.ID_FIELD_NUMBER);
    this.validateRuleData(request);
  }

  void validateOrThrow(RequestContext requestContext, RankSpanIngestionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, RankSpanIngestionRuleRequest.ID_TO_UPDATE_FIELD_NUMBER);
    if (request.getIdToUpdate().equals(request.getPrecedingRuleId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Can't rerank a rule against itself: %s", request))
          .asRuntimeException();
    }
  }

  private void validateRule(CreateSpanIngestionRuleRequest request) {
    switch (request.getRuleCase()) {
      case REQUEST_HEADER_RULE:
        validateKeyValueRetentionRuleData(request.getRequestHeaderRule());
        return;
      case RESPONSE_HEADER_RULE:
        validateKeyValueRetentionRuleData(request.getResponseHeaderRule());
        return;
      case ATTRIBUTE_RULE:
        validateKeyValueRetentionRuleData(request.getAttributeRule());
        return;
      case RULE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid span ingestion rule received with unrecognized case: %s", request))
            .asRuntimeException();
    }
  }

  private void validateKeyValueRetentionRuleData(KeyValueRetentionRuleData data) {
    validateRetentionAction(data.getAction());
    if (data.hasPredicate()) {
      validatePredicate(data.getPredicate());
    }
  }

  private void validateRetentionAction(RetentionAction action) {
    switch (action.getActionCase()) {
      case RETAIN:
      case DISCARD:
        return;
      case ACTION_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid retention action received with unrecognized case: %s", action))
            .asRuntimeException();
    }
  }

  private void validatePredicate(Predicate predicate) {
    switch (predicate.getPredicateCase()) {
      case TARGET_KEY_PREDICATE:
        validateStringPredicate(predicate.getTargetKeyPredicate());
        return;
      case PREDICATE_NOT_SET:
        return;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Unsupported predicate case %s", predicate))
            .asRuntimeException();
    }
  }

  private void validateStringPredicate(StringPredicate stringPredicate) {
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(stringPredicate, StringPredicate.VALUE_FIELD_NUMBER);
  }

  private void validateRuleData(UpdateSpanIngestionRuleRequest request) {
    switch (request.getRuleCase()) {
      case KEY_VALUE_RETENTION_RULE_DATA:
        this.validateKeyValueRetentionRuleData(request.getKeyValueRetentionRuleData());
        return;
      case RULE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid rule received with unrecognized case: %s", request))
            .asRuntimeException();
    }
  }
}
