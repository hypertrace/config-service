package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import static ai.traceable.config.utils.RegexValidator.validateRegex;
import static ai.traceable.customsignature.config.service.v1.KeyValueTag.KEY_VALUE_TAG_UNSPECIFIED;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.EntityType;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Singleton
public class ModsecClauseConverterImpl implements ModsecClauseConverter {
  private static final String OR_REGEX_DELIMITER = "|";
  private final CachedServiceMappingProvider cachedServiceMappingProvider;

  @Inject
  public ModsecClauseConverterImpl(CachedServiceMappingProvider cachedServiceMappingProvider) {
    this.cachedServiceMappingProvider = cachedServiceMappingProvider;
  }

  @Override
  public ModsecClauseResult convert(RequestContext requestContext, DetectionExclusionRule rule) {
    Deque<Clause> clauses = new LinkedList<>();
    List<ServiceDetail> serviceDetails = new ArrayList<>();
    rule.getRuleInfo()
        .getConditionsList()
        .forEach(
            condition -> {
              switch (condition.getConditionCase()) {
                case SCOPE_CONDITION:
                  serviceDetails.addAll(handleScopeCondition(requestContext, condition, clauses));
                  break;
                case ATTRIBUTE_MATCH_CONDITION:
                  clauses.addLast(buildAttributeClause(condition.getAttributeMatchCondition()));
                  break;
                case IP_LOCATION_TYPE_CONDITION:
                case EVENT_CONDITION:
                case ANOMALOUS_ATTRIBUTE_CONDITION:
                case IP_ADDRESS_CONDITION:
                case REGION_CONDITION:
                  // Handled in blocking config i.e. outside modsec
                  break;
                default:
                  throw new UnsupportedOperationException(
                      String.format(
                          "Cannot convert condition of type %s for blocking exclusion rule for tenant: %s",
                          condition.getConditionCase(), requestContext.getTenantId()));
              }
            });
    return new ModsecClauseResult(
        clauses.stream().collect(Collectors.toUnmodifiableList()), serviceDetails);
  }

  private List<ServiceDetail> handleScopeCondition(
      RequestContext requestContext, DetectionExclusionCondition condition, Deque<Clause> clauses) {
    var scopeCondition = condition.getScopeCondition();
    if (scopeCondition.hasUrlScope()) {
      // Adding URL clauses at the beginning since they are less expensive to evaluate
      buildUrlRegexClause(
              scopeCondition.getUrlScope().getUrlRegexesList(), scopeCondition.getExclude())
          .forEach(clauses::addFirst);
      return List.of();
    } else if (scopeCondition.hasEntityScope()
        && scopeCondition.getEntityScope().getEntityType() == EntityType.ENTITY_TYPE_SERVICE) {
      return scopeCondition.getEntityScope().getEntityIdsList().stream()
          .distinct()
          .map(id -> cachedServiceMappingProvider.getServiceIdentifierEntity(requestContext, id))
          .flatMap(Optional::stream)
          .map(
              serviceIdentifierEntity ->
                  new ServiceDetail(
                      serviceIdentifierEntity.getServiceName(), scopeCondition.getExclude()))
          .collect(Collectors.toUnmodifiableList());
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot handle scope conditions - %s while converting to modsec", scopeCondition));
    }
  }

  private List<Clause> buildUrlRegexClause(Collection<String> urlRegexes, boolean excludeMatch) {
    if (!excludeMatch) {
      // Combine regexes using OR delimiter
      String combinedRegex = String.join(OR_REGEX_DELIMITER, urlRegexes);
      validateRegex(combinedRegex);

      return Collections.singletonList(
          buildUrlClause(
              combinedRegex,
              ai.traceable.customsignature.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_MATCHES_REGEX));
    }

    // If exclude match is true, construct a list of not matches
    return urlRegexes.stream()
        .map(
            urlRegex ->
                buildUrlClause(
                    urlRegex,
                    ai.traceable.customsignature.config.service.v1.MatchOperator
                        .MATCH_OPERATOR_NOT_MATCH_REGEX))
        .collect(Collectors.toUnmodifiableList());
  }

  private Clause buildAttributeClause(SpanAttributeMatchCondition spanAttributeCondition) {
    ClauseDetails extractedClauseDetails =
        convertMetadata(spanAttributeCondition.getKeyMatchCondition().getMetadata());

    boolean hasKeyMatch = spanAttributeCondition.getKeyMatchCondition().hasMatchCondition();
    boolean hasValueMatch = spanAttributeCondition.hasValueMatchCondition();
    boolean hasUserAgentKeyMetadata =
        spanAttributeCondition.getKeyMatchCondition().getMetadata()
            == KeyMetadata.KEY_METADATA_USER_AGENT;

    if (hasKeyMatch && hasValueMatch && !hasUserAgentKeyMetadata) {
      return buildKeyValueClause(spanAttributeCondition, extractedClauseDetails);
    } else if (hasKeyMatch || hasValueMatch) {
      return buildMatchExpressionClause(spanAttributeCondition, extractedClauseDetails);
    }

    throw new UnsupportedOperationException(
        String.format(
            "Cannot convert span attribute condition - %s, into clause", spanAttributeCondition));
  }

  private Clause buildKeyValueClause(
      SpanAttributeMatchCondition spanAttributeCondition, ClauseDetails extractedClauseDetails) {
    return Clause.newBuilder()
        .setKeyValueExpression(
            KeyValueExpression.newBuilder()
                .setTag(
                    extractedClauseDetails
                        .getKeyValueTagOptional()
                        .orElse(KEY_VALUE_TAG_UNSPECIFIED))
                .setMatchCategory(extractedClauseDetails.getCategory())
                .setMatchKey(
                    spanAttributeCondition
                        .getKeyMatchCondition()
                        .getMatchCondition()
                        .getValue()
                        .getStringValue())
                .setKeyMatchOperator(
                    convertOperator(
                        spanAttributeCondition
                            .getKeyMatchCondition()
                            .getMatchCondition()
                            .getOperator()))
                .setMatchValue(
                    spanAttributeCondition.getValueMatchCondition().getValue().getStringValue())
                .setValueMatchOperator(
                    convertOperator(spanAttributeCondition.getValueMatchCondition().getOperator())))
        .build();
  }

  private Clause buildMatchExpressionClause(
      SpanAttributeMatchCondition spanAttributeCondition, ClauseDetails extractedClauseDetails) {
    MatchExpression.Builder expressionBuilder =
        MatchExpression.newBuilder()
            .setMatchCategory(extractedClauseDetails.getCategory())
            .setMatchKey(extractedClauseDetails.getKeyType());

    if (spanAttributeCondition.getKeyMatchCondition().hasMatchCondition()) {
      var keyMatch = spanAttributeCondition.getKeyMatchCondition().getMatchCondition();
      expressionBuilder
          .setMatchOperator(convertOperator(keyMatch.getOperator()))
          .setMatchValue(keyMatch.getValue().getStringValue());
    } else {
      var valueMatch = spanAttributeCondition.getValueMatchCondition();
      expressionBuilder
          .setMatchOperator(convertOperator(valueMatch.getOperator()))
          .setMatchValue(valueMatch.getValue().getStringValue());
    }

    return Clause.newBuilder().setMatchExpression(expressionBuilder).build();
  }

  private Clause buildUrlClause(
      String regex, ai.traceable.customsignature.config.service.v1.MatchOperator operator) {
    return Clause.newBuilder()
        .setMatchExpression(
            MatchExpression.newBuilder()
                .setMatchKey(MatchKey.MATCH_KEY_URL)
                .setMatchOperator(operator)
                .setMatchValue(regex)
                .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST))
        .build();
  }

  private ClauseDetails convertMetadata(KeyMetadata metadata) {
    switch (metadata) {
      case KEY_METADATA_URL:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_URL,
            Optional.empty(),
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_HOST:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_HOST,
            Optional.empty(),
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_HTTP_METHOD:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_HTTP_METHOD,
            Optional.empty(),
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_USER_AGENT:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_HEADER_NAME,
            Optional.of(MatchKey.MATCH_KEY_HEADER_VALUE),
            Optional.of(KeyValueTag.KEY_VALUE_TAG_HEADER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_STATUS_CODE:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_STATUS_CODE,
            Optional.empty(),
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case KEY_METADATA_REQUEST_BODY:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_BODY,
            Optional.empty(),
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_RESPONSE_BODY:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_BODY,
            Optional.empty(),
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case KEY_METADATA_REQUEST_HEADER:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_HEADER_NAME,
            Optional.of(MatchKey.MATCH_KEY_HEADER_VALUE),
            Optional.of(KeyValueTag.KEY_VALUE_TAG_HEADER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_RESPONSE_HEADER:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_HEADER_NAME,
            Optional.of(MatchKey.MATCH_KEY_HEADER_VALUE),
            Optional.of(KeyValueTag.KEY_VALUE_TAG_HEADER),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case KEY_METADATA_REQUEST_COOKIE:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_COOKIE_NAME,
            Optional.of(MatchKey.MATCH_KEY_COOKIE_VALUE),
            Optional.of(KeyValueTag.KEY_VALUE_TAG_COOKIE),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_QUERY_PARAMETER:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_QUERY_PARAMETER_NAME,
            Optional.of(MatchKey.MATCH_KEY_QUERY_PARAMETER_VALUE),
            Optional.of(KeyValueTag.KEY_VALUE_TAG_QUERY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_REQUEST_BODY_PARAMETER:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_BODY_PARAMETER_NAME,
            Optional.of(MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE),
            Optional.of(KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_METADATA_RESPONSE_BODY_PARAMETER:
        return new ClauseDetails(
            MatchKey.MATCH_KEY_BODY_PARAMETER_NAME,
            Optional.of(MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE),
            Optional.of(KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert a condition of metadata:%s into clause", metadata));
    }
  }

  private static final EnumMap<
          MatchOperator, ai.traceable.customsignature.config.service.v1.MatchOperator>
      spanMatchOperatorMapping =
          new EnumMap<>(
              Map.of(
                  MatchOperator.MATCH_OPERATOR_EQUALS,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_EQUALS,
                  MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_NOT_EQUAL,
                  MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_MATCHES_REGEX,
                  MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_NOT_MATCH_REGEX,
                  MatchOperator.MATCH_OPERATOR_GREATER_THAN,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_GREATER_THAN,
                  MatchOperator.MATCH_OPERATOR_LESS_THAN,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_LESS_THAN));

  private static ai.traceable.customsignature.config.service.v1.MatchOperator convertOperator(
      MatchOperator matchOperator) {
    return Optional.ofNullable(spanMatchOperatorMapping.get(matchOperator))
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format(
                        "Cannot convert an operator of type:%s into modsec rule", matchOperator)));
  }

  @Value
  static class ClauseDetails {
    MatchKey keyType;
    Optional<MatchKey> valueTypeOptional;
    Optional<KeyValueTag> KeyValueTagOptional;
    MatchCategory category;
  }
}
