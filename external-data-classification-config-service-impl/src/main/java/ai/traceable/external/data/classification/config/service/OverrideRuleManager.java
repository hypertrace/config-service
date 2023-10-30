package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.DataSuppressionOverride;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.SpanFilter;
import ai.traceable.data.classification.config.service.v1.SpanKeyValueFilter;
import ai.traceable.data.classification.config.service.v1.SpanLogicalFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.EnvironmentFilter;
import ai.traceable.external.data.classification.config.service.v1.Operator;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class OverrideRuleManager {
  private final DataClassificationRulesDao dataClassificationRulesDao;
  private final DataClassificationRulesTranslator dataClassificationRulesTranslator;

  List<DataClassificationOverride> getOverrides(
      RequestContext requestContext, EnvironmentFilter environmentFilter) {
    if (environmentFilter.getEnvironmentName().isBlank()) {
      return this.dataClassificationRulesDao.getUnscopedDataSuppressionOverrideRules(
          requestContext);
    }
    return this.dataClassificationRulesDao.getDataSuppressionOverrideRulesForEnvironment(
        requestContext, environmentFilter.getEnvironmentName());
  }

  List<DataType> applyOverrides(
      List<DataType> dataTypes, List<DataClassificationOverride> overrides) {
    return dataTypes.stream()
        .flatMap(dataType -> this.applyOverridesToDatatype(dataType, overrides))
        .collect(Collectors.toUnmodifiableList());
  }

  List<DataType> applyOverridesAndFilterRawRules(
      List<DataType> dataTypes, List<DataClassificationOverride> overrides) {
    return this.filterRawRules(this.applyOverrides(dataTypes, overrides));
  }

  private List<DataType> filterRawRules(List<DataType> dataTypes) {
    // remove raw rules unless they are a session identifier or only apply to a subset of spans (as
    // this may indicate an override to a later transformation)
    return dataTypes.stream()
        .filter(
            type ->
                type.getSessionIdentifier()
                    || type.hasTransformation()
                    || type.getMatchRulesList().stream().anyMatch(DataTypeMatchRule::hasSpanFilter))
        .collect(Collectors.toUnmodifiableList());
  }

  private Stream<DataType> applyOverridesToDatatype(
      DataType originalDataType, List<DataClassificationOverride> overrides) {
    // Each override can potentially match a different set of criteria and change the behavior in
    // different ways.
    // so we produce a new data type for each override that matches that declared criteria and
    // behavior, then if it's
    // possible to not match (i.e. the override is conditional), we also add the original data type
    // back to match with lower priority than the override.
    List<DataClassificationOverride> usableOverrides =
        this.filterToUsableOverrides(originalDataType, overrides);
    return Streams.concat(
        this.buildDataTypesWithOverrides(originalDataType, usableOverrides),
        this.buildOriginalDataWithoutOverrides(originalDataType, usableOverrides).stream());
  }

  private Optional<DataType> buildOriginalDataWithoutOverrides(
      DataType originalDataType, List<DataClassificationOverride> overrides) {
    // TODO - explore inverting overrides to avoid evaluating data type if it's already been
    // checked. This is challenging however since agents have limited operator support. A rule
    // containing an AND (like a basic key match AND value match rule) requires an OR to invert.
    // Ultimately though, this is just an optimization. If this rule is reached, the override
    // condition has already been checked, so we know this data type is not applicable and is
    // eligible to be skipped

    // For now, we only do the most basic case - any unconditional override allows us to remove the
    // original type.
    if (overrides.stream().anyMatch(this::isUnconditional)) {
      return Optional.empty();
    }
    return Optional.of(originalDataType);
  }

  /**
   * This builds the data type that will check the intersection of when the data type and each
   * override apply. For example, if the override changes the behavior when a certain IP address is
   * present, this will create a data type matching that IP address with the overridden behavior.
   */
  private Stream<DataType> buildDataTypesWithOverrides(
      DataType originalDataType, List<DataClassificationOverride> overrides) {

    return overrides.stream()
        .map(
            override ->
                this.isUnconditional(override)
                    ? Optional.of(
                        this.buildUnconditionalOverrideDataType(
                            originalDataType,
                            override
                                .getDataClassificationOverrideRule()
                                .getDataSuppressionOverride()))
                    : this.buildConditionalOverrideDataType(
                        originalDataType,
                        override.getDataClassificationOverrideRule().getDataSuppressionOverride(),
                        override.getDataClassificationOverrideRule().getSpanFilter()))
        .flatMap(Optional::stream);
  }

  private boolean isUnconditional(DataClassificationOverride override) {
    return !override.getDataClassificationOverrideRule().hasSpanFilter();
  }

  private List<DataClassificationOverride> filterToUsableOverrides(
      DataType originalDataType, List<DataClassificationOverride> overrides) {
    // First remove the overrides that aren't supported.
    List<DataClassificationOverride> usableOverrides =
        overrides.stream()
            .filter(
                override -> {
                  switch (override.getDataClassificationOverrideRule().getOverrideCase()) {
                    case DATA_SUPPRESSION_OVERRIDE:
                      return true;
                    case OVERRIDE_NOT_SET:
                      log.warn(
                          "Unexpected unset Data Classification override for overrides: {}",
                          overrides);
                      return false;
                    default:
                      log.warn(
                          "Ignoring unknown Data Classification override for overrides: {}",
                          overrides);
                      return false;
                  }
                })
            .collect(Collectors.toUnmodifiableList());

    // Next remove any overrides following an unconditional override
    int indexOfUnconditional = Iterables.indexOf(usableOverrides, this::isUnconditional);
    boolean hasUnconditional = indexOfUnconditional >= 0;
    usableOverrides =
        hasUnconditional
            ? usableOverrides.subList(0, indexOfUnconditional + 1) // sublist toIndex is exclusive
            : usableOverrides;

    // Then trim overrides at the end that don't change behavior of the original data type
    usableOverrides =
        this.removingTrailingOverridesWithMatchingTransformationType(
            usableOverrides, originalDataType.getTransformation());
    // Merge any overrides together (simplified to any conditional overrides directly preceding
    // a final unconditional override)
    if (!usableOverrides.isEmpty() && this.isUnconditional(Iterables.getLast(usableOverrides))) {
      DataClassificationOverride unconditionalOverride = Iterables.getLast(usableOverrides);
      DataTransformation unconditionalTransformationType =
          this.dataClassificationRulesTranslator.translateDataSuppression(
              unconditionalOverride
                  .getDataClassificationOverrideRule()
                  .getDataSuppressionOverride()
                  .getDataSuppression());

      usableOverrides =
          ImmutableList.<DataClassificationOverride>builder()
              .addAll(
                  this.removingTrailingOverridesWithMatchingTransformationType(
                      usableOverrides, unconditionalTransformationType))
              .add(unconditionalOverride)
              .build();
    }
    // And finally, remove any exact duplicates
    return usableOverrides.stream().distinct().collect(Collectors.toUnmodifiableList());
  }

  private Optional<DataType> buildConditionalOverrideDataType(
      DataType originalDataType, DataSuppressionOverride suppressionOverride, SpanFilter filter) {
    return this.translateFilter(filter)
        .map(
            translatedFilter ->
                this.buildConditionalOverrideDataType(
                    originalDataType, suppressionOverride, translatedFilter));
  }

  private DataType buildConditionalOverrideDataType(
      DataType originalDataType,
      DataSuppressionOverride suppressionOverride,
      FilterTranslationResult filter) {
    List<DataTypeMatchRule> filteredMatchRules =
        originalDataType.getMatchRulesList().stream()
            .flatMap(matchRule -> this.buildFilteredMatchRules(matchRule, filter))
            .collect(Collectors.toUnmodifiableList());

    return this.buildUnconditionalOverrideDataType(originalDataType, suppressionOverride)
        .toBuilder()
        .clearMatchRules()
        .addAllMatchRules(filteredMatchRules)
        .build();
  }

  private Stream<DataTypeMatchRule> buildFilteredMatchRules(
      DataTypeMatchRule matchRule, FilterTranslationResult filter) {
    return filter.getIndependentFilters().stream()
        .map(
            externalFilter ->
                matchRule.toBuilder()
                    .setSpanFilter(
                        matchRule.getSpanFilter().toBuilder()
                            .addAllRequiredMatchingAttributes(
                                externalFilter.getRequiredMatchingAttributesList()))
                    .build());
  }

  private DataType buildUnconditionalOverrideDataType(
      DataType originalDataType, DataSuppressionOverride suppressionOverride) {
    DataTransformation overriddenTransformation =
        this.dataClassificationRulesTranslator.translateDataSuppression(
            suppressionOverride.getDataSuppression());

    if (overriddenTransformation == DataTransformation.DATA_TRANSFORMATION_UNSPECIFIED) {
      return originalDataType.toBuilder().clearTransformation().build();
    }
    return originalDataType.toBuilder().setTransformation(overriddenTransformation).build();
  }

  private List<DataClassificationOverride> removingTrailingOverridesWithMatchingTransformationType(
      List<DataClassificationOverride> overridesToCheck,
      DataTransformation transformationTypeToRemove) {
    long trailingElementsToRemove =
        Lists.reverse(overridesToCheck).stream()
            .map(DataClassificationOverride::getDataClassificationOverrideRule)
            .map(DataClassificationOverrideRule::getDataSuppressionOverride)
            .map(DataSuppressionOverride::getDataSuppression)
            .map(this.dataClassificationRulesTranslator::translateDataSuppression)
            .takeWhile(transformationTypeToRemove::equals)
            .count();

    return overridesToCheck.subList(
        0, Math.toIntExact(overridesToCheck.size() - trailingElementsToRemove));
  }

  private Optional<FilterTranslationResult> translateFilter(SpanFilter filter) {
    switch (filter.getFilterCase()) {
      case LOGICAL_FILTER:
        return this.translateLogicalFilter(filter.getLogicalFilter())
            .filter(FilterTranslationResult::containsFilters);
      case KEY_VALUE_FILTER:
        return this.translateKeyValuePattern(filter.getKeyValueFilter())
            .filter(FilterTranslationResult::containsFilters);
      case NEGATION_FILTER:
        return this.translateNegatedFilter(filter.getNegationFilter())
            .filter(FilterTranslationResult::containsFilters);
      case FILTER_NOT_SET:
        return Optional.empty();
      default:
        log.warn("Dropping unrecognized data classification override span filter: {}", filter);
        return Optional.empty();
    }
  }

  private FilterTranslationResult buildTranslationResultFromPredicate(
      AttributePredicate attributePredicate) {
    return new FilterTranslationResult(
        List.of(
            ai.traceable.external.data.classification.config.service.v1.SpanFilter.newBuilder()
                .addRequiredMatchingAttributes(attributePredicate)
                .build()));
  }

  private Optional<FilterTranslationResult> translateLogicalFilter(
      SpanLogicalFilter logicalFilter) {
    switch (logicalFilter.getOperator()) {
      case LOGICAL_OPERATOR_AND:
        return this.translateLogicalAndFilter(logicalFilter.getChildrenList());
      case UNRECOGNIZED:
      case LOGICAL_OPERATOR_UNSPECIFIED:
      default:
        log.warn("Unsupported logical filter case: {}", logicalFilter);
        return Optional.empty();
    }
  }

  private Optional<FilterTranslationResult> translateLogicalAndFilter(
      List<SpanFilter> filterChildren) {
    List<FilterTranslationResult> childTranslations =
        filterChildren.stream()
            .map(this::translateFilter)
            .flatMap(Optional::stream)
            .filter(FilterTranslationResult::canBeRepresentedBySingleFilter)
            .collect(Collectors.toUnmodifiableList());
    // If any child is unsupported or requires multiple filters (OR), then we can't correctly
    // represent it, so drop the whole thing
    if (childTranslations.size() != filterChildren.size()) {
      log.warn(
          "Dropping logical AND filter that has at least one unsupported child: {}",
          filterChildren);
      return Optional.empty();
    }
    // Otherwise, collect all the predicates and merge into a single filter.
    List<AttributePredicate> collectedPredicates =
        childTranslations.stream()
            .map(FilterTranslationResult::getOnlyFilter)
            .map(
                ai.traceable.external.data.classification.config.service.v1.SpanFilter
                    ::getRequiredMatchingAttributesList)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());

    FilterTranslationResult mergedResult =
        new FilterTranslationResult(
            List.of(
                ai.traceable.external.data.classification.config.service.v1.SpanFilter.newBuilder()
                    .addAllRequiredMatchingAttributes(collectedPredicates)
                    .build()));
    return Optional.of(mergedResult);
  }

  private Optional<FilterTranslationResult> translateKeyValuePattern(
      SpanKeyValueFilter keyValuePattern) {
    AttributePredicate.Builder builder = AttributePredicate.newBuilder();
    if (keyValuePattern.hasKeyPattern()) {
      this.translateStringPattern(keyValuePattern.getKeyPattern())
          .ifPresent(builder::setNamePredicate);
    }
    if (keyValuePattern.hasValuePattern()) {
      this.translateStringPattern(keyValuePattern.getValuePattern())
          .ifPresent(builder::setValuePredicate);
    }

    return Optional.of(builder.build())
        .filter(predicate -> !predicate.equals(AttributePredicate.getDefaultInstance()))
        .map(this::buildTranslationResultFromPredicate);
  }

  private Optional<StringPredicate> translateStringPattern(StringPattern stringPattern) {
    StringPredicate.Builder builder =
        StringPredicate.newBuilder().setValue(stringPattern.getValue());
    switch (stringPattern.getOperator()) {
      case OPERATOR_EQUALS:
        return Optional.of(builder.setOperator(Operator.OPERATOR_EQUALS).build());
      case OPERATOR_MATCHES_REGEX:
        return Optional.of(builder.setOperator(Operator.OPERATOR_MATCHES_REGEX).build());
      case UNRECOGNIZED:
      case OPERATOR_UNSPECIFIED:
      default:
        log.warn("Unable to translate string pattern, unsupported operator {}", stringPattern);
        return Optional.empty();
    }
  }

  private Optional<FilterTranslationResult> translateNegatedFilter(SpanFilter negatedFilter) {
    switch (negatedFilter.getFilterCase()) {
      case LOGICAL_FILTER:
        // !(X AND Y) => !X OR !Y
        List<ai.traceable.external.data.classification.config.service.v1.SpanFilter>
            negatedIndependentChildren =
                negatedFilter.getLogicalFilter().getChildrenList().stream()
                    .map(this::translateNegatedFilter)
                    .flatMap(Optional::stream)
                    .map(FilterTranslationResult::getIndependentFilters)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toUnmodifiableList());
        return Optional.of(new FilterTranslationResult(negatedIndependentChildren))
            .filter(FilterTranslationResult::containsFilters);
      case KEY_VALUE_FILTER:
        return this.translateNegatedKeyValueFilter(negatedFilter.getKeyValueFilter());
      case NEGATION_FILTER:
        // double negation, just return inner filter - !!(filter) => filter
        return this.translateFilter(negatedFilter.getNegationFilter());
      case FILTER_NOT_SET:
      default:
        return Optional.empty();
    }
  }

  private Optional<FilterTranslationResult> translateNegatedKeyValueFilter(
      SpanKeyValueFilter keyValueFilter) {
    // TODO
    // Without more agent operators, it doesn't really make sense to negate a KV filter, since these
    // are currently evaluated independently against each KV pair
    // Key only - need to represent this as "no key matches k-arg"
    // value only - need to represent this as "no value matches v-arg"
    // key and value - translates to
    //   "no key matches k-arg OR (key matches k-arg AND value not_matches v_arg)".
    // Here, we'll do best effort and do the second part.
    if (keyValueFilter.hasKeyPattern() && keyValueFilter.hasValuePattern()) {
      AttributePredicate.Builder builder = AttributePredicate.newBuilder();

      this.translateStringPattern(keyValueFilter.getKeyPattern())
          .ifPresent(builder::setNamePredicate);
      this.translateNegatedStringPattern(keyValueFilter.getValuePattern())
          .ifPresent(builder::setValuePredicate);

      return Optional.of(builder.build())
          .filter(predicate -> predicate.hasNamePredicate() && predicate.hasValuePredicate())
          .map(this::buildTranslationResultFromPredicate);
    }

    log.warn("Unsupported key value filter for negation: {}", keyValueFilter);
    return Optional.empty();
  }

  private Optional<StringPredicate> translateNegatedStringPattern(StringPattern stringPattern) {
    StringPredicate.Builder builder =
        StringPredicate.newBuilder().setValue(stringPattern.getValue());
    switch (stringPattern.getOperator()) {
      case OPERATOR_EQUALS:
        return Optional.of(builder.setOperator(Operator.OPERATOR_NOT_EQUALS).build());
      case OPERATOR_MATCHES_REGEX:
        return Optional.of(builder.setOperator(Operator.OPERATOR_NOT_MATCHES_REGEX).build());
      case OPERATOR_UNSPECIFIED:
        log.warn("Missing operator: {}", stringPattern);
        return Optional.empty();
      case UNRECOGNIZED:
      default:
        log.warn("Unrecognized operator: {}", stringPattern);
        return Optional.empty();
    }
  }

  @AllArgsConstructor
  private static class FilterTranslationResult {
    // By independent, we mean a logical OR - any of these can be matched individually
    private final List<ai.traceable.external.data.classification.config.service.v1.SpanFilter>
        independentExternalFilters;

    boolean canBeRepresentedBySingleFilter() {
      return independentExternalFilters.size() == 1;
    }

    boolean containsFilters() {
      return !independentExternalFilters.isEmpty();
    }

    List<ai.traceable.external.data.classification.config.service.v1.SpanFilter>
        getIndependentFilters() {
      return this.independentExternalFilters;
    }

    ai.traceable.external.data.classification.config.service.v1.SpanFilter getOnlyFilter() {
      return Iterables.getOnlyElement(independentExternalFilters);
    }
  }
}
