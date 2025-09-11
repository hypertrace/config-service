package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.parsing.config.service.v1.DataParsingConfig;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import java.util.List;
import java.util.stream.Collectors;

public class DataParsingRuleConverter {

  public List<DataParsingRule> convert(List<DataParsingConfig> dataParsingConfigsList) {
    return dataParsingConfigsList.stream()
        .map(this::convert)
        .collect(Collectors.toUnmodifiableList());
  }

  private ai.traceable.external.data.classification.config.service.v1.DataParsingRule convert(
      DataParsingConfig config) {
    ai.traceable.data.parsing.config.service.v1.DataParsingRule src = config.getDataParsingRule();
    ai.traceable.external.data.classification.config.service.v1.DataParsingRule.Builder builder =
        ai.traceable.external.data.classification.config.service.v1.DataParsingRule.newBuilder();

    // Copy span_filter
    if (src.hasSpanFilter()) {
      builder.setSpanFilter(
          ai.traceable.external.data.classification.config.service.v1.SpanFilter.newBuilder()
              .addAllRequiredMatchingAttributes(
                  src.getSpanFilter().getRequiredMatchingAttributesList().stream()
                      .map(this::convertAttributePredicate)
                      .collect(Collectors.toList()))
              .build());
    }

    // Copy attribute_filter
    if (src.hasAttributeFilter()) {
      builder.setAttributeFilter(
          ai.traceable.external.data.classification.config.service.v1.AttributeFilter.newBuilder()
              .addAllPrefixes(src.getAttributeFilter().getPrefixesList())
              .build());
    }

    // Copy mode (enums are compatible by number)
    builder.setModeValue(src.getModeValue());

    // Copy failure_strategy (only DropOnFailure is supported)
    if (src.hasDropOnFailure()) {
      builder.setDropOnFailure(
          ai.traceable.external.data.classification.config.service.v1.DataParsingRule.DropOnFailure
              .getDefaultInstance());
    }

    return builder.build();
  }

  private ai.traceable.external.data.classification.config.service.v1.AttributePredicate
      convertAttributePredicate(
          ai.traceable.data.parsing.config.service.v1.AttributePredicate src) {
    ai.traceable.external.data.classification.config.service.v1.AttributePredicate.Builder builder =
        ai.traceable.external.data.classification.config.service.v1.AttributePredicate.newBuilder();
    if (src.hasNamePredicate()) {
      builder.setNamePredicate(convertStringPredicate(src.getNamePredicate()));
    }
    if (src.hasValuePredicate()) {
      builder.setValuePredicate(convertStringPredicate(src.getValuePredicate()));
    }
    return builder.build();
  }

  private ai.traceable.external.data.classification.config.service.v1.StringPredicate
      convertStringPredicate(ai.traceable.data.parsing.config.service.v1.StringPredicate src) {
    return ai.traceable.external.data.classification.config.service.v1.StringPredicate.newBuilder()
        .setOperatorValue(src.getOperatorValue())
        .setValue(src.getValue())
        .build();
  }
}
