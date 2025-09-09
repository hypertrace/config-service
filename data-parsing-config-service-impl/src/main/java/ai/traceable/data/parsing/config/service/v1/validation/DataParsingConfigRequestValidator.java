package ai.traceable.data.parsing.config.service.v1.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.parsing.config.service.v1.AttributeFilter;
import ai.traceable.data.parsing.config.service.v1.CreateDataParsingRuleRequest;
import ai.traceable.data.parsing.config.service.v1.DataParsingRule;
import ai.traceable.data.parsing.config.service.v1.DeleteDataParsingRuleRequest;
import ai.traceable.data.parsing.config.service.v1.GetDataParsingRulesRequest;
import ai.traceable.data.parsing.config.service.v1.RankDataParsingConfigRequest;
import ai.traceable.data.parsing.config.service.v1.SpanFilter;
import ai.traceable.data.parsing.config.service.v1.StringPredicate;
import ai.traceable.data.parsing.config.service.v1.UpdateDataParsingRuleRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataParsingConfigRequestValidator {
  public void validateOrThrow(RequestContext requestContext, CreateDataParsingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateOrThrow(request.getDataParsingRule());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateDataParsingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateDataParsingRuleRequest.ID_FIELD_NUMBER);
    validateOrThrow(request.getDataParsingRule());
  }

  void validateOrThrow(DataParsingRule dataParsingRule) {
    // Validate required fields
    validateNonDefaultPresenceOrThrow(dataParsingRule, DataParsingRule.MODE_FIELD_NUMBER);

    // Validate attribute_filter if present
    if (dataParsingRule.hasAttributeFilter()) {
      AttributeFilter filter = dataParsingRule.getAttributeFilter();
      if (filter.getPrefixesCount() == 0) {
        throw Status.INVALID_ARGUMENT
            .withDescription("AttributeFilter must have at least one prefix if set")
            .asRuntimeException();
      }
    }

    // Validate span_filter if present
    if (dataParsingRule.hasSpanFilter()) {
      SpanFilter spanFilter = dataParsingRule.getSpanFilter();
      if (spanFilter.getRequiredMatchingAttributesCount() == 0) {
        throw Status.INVALID_ARGUMENT
            .withDescription("SpanFilter must have at least one required_matching_attribute if set")
            .asRuntimeException();
      }
      // Validate each required_matching_attribute if present
      for (int i = 0; i < spanFilter.getRequiredMatchingAttributesCount(); i++) {
        // Only name_predicate is required, value_predicate is optional
        if (!spanFilter.getRequiredMatchingAttributes(i).hasNamePredicate()) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  "Each required_matching_attribute in SpanFilter must have a name_predicate")
              .asRuntimeException();
        }
        // Validate StringPredicate for name_predicate
        validateStringPredicate(spanFilter.getRequiredMatchingAttributes(i).getNamePredicate());
        // Validate StringPredicate for value_predicate if present
        if (spanFilter.getRequiredMatchingAttributes(i).hasValuePredicate()) {
          validateStringPredicate(spanFilter.getRequiredMatchingAttributes(i).getValuePredicate());
        }
      }
    }

    // Validate scope (if present)
    if (dataParsingRule.hasEnvironmentScope()) {
      if (dataParsingRule.getEnvironmentScope().getEnvironmentIdsCount() == 0) {
        throw Status.INVALID_ARGUMENT
            .withDescription("EnvironmentScope must have at least one environment_id if set")
            .asRuntimeException();
      }
    }
    // No-op for global_scope (empty message)
  }

  private void validateStringPredicate(StringPredicate predicate) {
    validateNonDefaultPresenceOrThrow(predicate, StringPredicate.VALUE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(predicate, StringPredicate.OPERATOR_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, DeleteDataParsingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteDataParsingRuleRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, GetDataParsingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, RankDataParsingConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, RankDataParsingConfigRequest.CONFIG_ID_TO_UPDATE_FIELD_NUMBER);
  }
}
