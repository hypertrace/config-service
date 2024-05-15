package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import static ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobConverterUtils.validateRegex;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobConverterUtils.ClauseDetails;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScopedPatternConverter {
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);

  private static final List<MatchKey> NESTED_MODSEC_KEY_TYPES =
      List.of(MatchKey.MATCH_KEY_BODY_PARAMETER_NAME);
  private static final List<Location> REQUEST_LOCATIONS =
      ImmutableList.of(
          Location.LOCATION_QUERY,
          Location.LOCATION_REQUEST_HEADER,
          Location.LOCATION_REQUEST_COOKIE,
          Location.LOCATION_REQUEST_BODY);

  List<Clause> convert(ScopedPattern scopedPattern, String dataTypeId) {
    try {
      return buildClausesScopePattern(scopedPattern);
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.warn(
            "Error while converting scopedPattern for dataTypeId: {} - {}. Skipping.",
            dataTypeId,
            scopedPattern);
      } else {
        log.debug(
            "Error while converting scopedPattern for dataTypeId: {} - {}. Skipping.",
            dataTypeId,
            scopedPattern,
            e);
      }
      return Collections.emptyList();
    }
  }

  private List<Clause> buildClausesScopePattern(ScopedPattern scopedPattern) {
    return scopedPattern.getLocationsList().stream()
        .map(
            location ->
                location.equals(Location.LOCATION_ANY)
                    ? REQUEST_LOCATIONS
                    : Collections.singletonList(location))
        .flatMap(List::stream)
        .distinct()
        .map(this::convertLocation)
        .map(clauseDetails -> buildClauseScopePatternForLocation(clauseDetails, scopedPattern))
        .collect(Collectors.toUnmodifiableList());
  }

  private Clause buildClauseScopePatternForLocation(
      ClauseDetails clauseDetails, ScopedPattern scopedPattern) {
    switch (scopedPattern.getPatternCase()) {
      case KEY_PATTERN:
        return convertKeyClause(clauseDetails, scopedPattern.getKeyPattern(), false);
      case LEAF_KEY_PATTERN:
        return convertKeyClause(clauseDetails, scopedPattern.getKeyPattern(), true);
      case KEY_VALUE_PATTERN:
        return convertKeyValueClause(
            clauseDetails,
            scopedPattern.getKeyValuePattern().getKeyPattern(),
            scopedPattern.getKeyValuePattern().getValuePattern(),
            false);
      case LEAF_KEY_VALUE_PATTERN:
        return convertKeyValueClause(
            clauseDetails,
            scopedPattern.getLeafKeyValuePattern().getKeyPattern(),
            scopedPattern.getLeafKeyValuePattern().getValuePattern(),
            true);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert datatype rule with scoped pattern %s", scopedPattern));
    }
  }

  private Clause convertKeyClause(
      ClauseDetails clauseDetails, StringPattern keyPattern, boolean isLeaf) {
    MatchKey matchKeyType = clauseDetails.getKeyTypeOptional().orElseThrow(RuntimeException::new);
    MatchOperator matchOperator = getMatchOperator(keyPattern.getOperator());
    String matchValue = keyPattern.getValue();

    if (NESTED_MODSEC_KEY_TYPES.contains(matchKeyType)) {
      matchValue =
          KeyRegexConverters.transformNestedParamNameRegex(
              matchValue, keyPattern.getOperator().equals(Operator.OPERATOR_MATCHES_REGEX), isLeaf);
      matchOperator = MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
    }

    // Check if formed regex are valid
    validateRegex(matchValue);

    return Clause.newBuilder()
        .setMatchExpression(
            MatchExpression.newBuilder()
                .setMatchKey(matchKeyType)
                .setMatchOperator(matchOperator)
                .setMatchValue(matchValue)
                .setMatchCategory(clauseDetails.getCategory()))
        .build();
  }

  private Clause convertKeyValueClause(
      ClauseDetails clauseDetails,
      StringPattern keyPattern,
      StringPattern valuePattern,
      boolean isLeaf) {
    MatchKey matchKeyType = clauseDetails.getKeyTypeOptional().orElseThrow(RuntimeException::new);
    KeyValueTag keyValueTag =
        clauseDetails.getKeyValueTagOptional().orElseThrow(RuntimeException::new);
    MatchOperator keyOperator = getMatchOperator(keyPattern.getOperator());
    String matchKey = keyPattern.getValue();

    if (NESTED_MODSEC_KEY_TYPES.contains(matchKeyType)) {
      matchKey =
          KeyRegexConverters.transformNestedParamNameRegex(
              matchKey, keyPattern.getOperator().equals(Operator.OPERATOR_MATCHES_REGEX), isLeaf);
      keyOperator = MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
    }

    // Check if formed regexes are valid
    validateRegex(matchKey);
    validateRegex(valuePattern.getValue());

    return Clause.newBuilder()
        .setKeyValueExpression(
            KeyValueExpression.newBuilder()
                .setTag(keyValueTag)
                .setMatchKey(matchKey)
                .setKeyMatchOperator(keyOperator)
                .setMatchValue(valuePattern.getValue())
                .setValueMatchOperator(getMatchOperator(valuePattern.getOperator()))
                .setMatchCategory(clauseDetails.getCategory()))
        .build();
  }

  private ClauseDetails convertLocation(Location location) {
    switch (location) {
      case LOCATION_PATH:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_URL,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case LOCATION_REQUEST_HEADER:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_HEADER_NAME),
            MatchKey.MATCH_KEY_HEADER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_HEADER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case LOCATION_RESPONSE_HEADER:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_HEADER_NAME),
            MatchKey.MATCH_KEY_HEADER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_HEADER),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case LOCATION_REQUEST_COOKIE:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_COOKIE_NAME),
            MatchKey.MATCH_KEY_COOKIE_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_COOKIE),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case LOCATION_QUERY:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_QUERY_PARAMETER_NAME),
            MatchKey.MATCH_KEY_QUERY_PARAMETER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_QUERY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case LOCATION_REQUEST_BODY:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_BODY_PARAMETER_NAME),
            MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case LOCATION_RESPONSE_BODY:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_BODY_PARAMETER_NAME),
            MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case LOCATION_RESPONSE_COOKIE:
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot convert a condition with location:%s into modsec rule", location));
    }
  }

  private static MatchOperator getMatchOperator(Operator operator) {
    return operator.equals(Operator.OPERATOR_MATCHES_REGEX)
        ? MatchOperator.MATCH_OPERATOR_MATCHES_REGEX
        : MatchOperator.MATCH_OPERATOR_EQUALS;
  }
}
