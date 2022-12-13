package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import static ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX;
import static ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_NOT_MATCHES_REGEX;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.auth.detection.config.service.v1.Predicate.KeyValuePredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.PredicateCase;
import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalPredicate;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class HeaderAuthDetectionRuleTranslator extends AuthDetectionRuleTranslator {
  // Match the "^" literal char as long as it's not preceded by a backslash escape char
  private static final String START_ANCHOR_MATCH = "(?<!\\\\)\\^";
  private final PredicateBuilder predicateBuilder;

  @Inject
  HeaderAuthDetectionRuleTranslator(
      AttributeRuleBuilder attributeRuleBuilder, PredicateBuilder predicateBuilder) {
    super(attributeRuleBuilder);
    this.predicateBuilder = predicateBuilder;
  }

  @Override
  public PredicateCase getPredicateCase() {
    return PredicateCase.HEADER_PREDICATE;
  }

  @Override
  public Optional<Predicate> translatePredicate(
      ai.traceable.auth.detection.config.service.v1.Predicate predicate) {
    return this.getHeaderMatchPredicates(predicate.getHeaderPredicate())
        .map(
            headerMatchPredicates ->
                Predicate.newBuilder()
                    .setLogicalPredicate(
                        LogicalPredicate.newBuilder()
                            .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                            .addAllChildren(headerMatchPredicates))
                    .build());
  }

  Optional<List<Predicate>> getHeaderMatchPredicates(KeyValuePredicate headerPredicate) {
    // Headers can show up in different attributes, so create a predicate for each possibility
    List<Predicate> predicateList =
        REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
            .map(
                formatString ->
                    this.rewriteHeaderPredicatesAsAttributeMatch(formatString, headerPredicate))
            .map(this.predicateBuilder::translateAttributeKeyValuePredicate)
            .flatMap(Optional::stream)
            .collect(toUnmodifiableList());

    if (predicateList.size() != REQUEST_HEADER_KEY_FORMAT_STRINGS.size()) {
      // If mismatched, then something failed to translate so omit the whole thing
      return Optional.empty();
    }
    return Optional.of(predicateList);
  }

  private KeyValuePredicate rewriteHeaderPredicatesAsAttributeMatch(
      String keyFormatString, KeyValuePredicate keyValuePredicate) {
    // If the user provides a regex to match a header name, but we embed that in a regex
    // to match an attribute of the format attr.key.<header>, we want to remove start anchor matches
    // since they won't be the start and that's already the behavior anyway
    String matchValue =
        List.of(RELATIONAL_OPERATOR_MATCHES_REGEX, RELATIONAL_OPERATOR_NOT_MATCHES_REGEX)
                .contains(keyValuePredicate.getKeyPredicate().getOperator())
            ? stripStartAnchorCharsFromRegexString(keyValuePredicate.getKeyPredicate().getValue())
            : keyValuePredicate.getKeyPredicate().getValue();

    KeyValuePredicate.Builder builder = keyValuePredicate.toBuilder();
    // Update with the full attribute key
    builder.getKeyPredicateBuilder().setValue(String.format(keyFormatString, matchValue));
    return builder.build();
  }

  private String stripStartAnchorCharsFromRegexString(String regexString) {
    return regexString.replaceFirst(START_ANCHOR_MATCH, "");
  }
}
