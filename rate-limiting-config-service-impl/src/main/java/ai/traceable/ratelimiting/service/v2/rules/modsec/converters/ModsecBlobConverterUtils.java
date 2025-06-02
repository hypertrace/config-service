package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.customsignature.config.service.modsec.CustomModsecRuleConverter;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class ModsecBlobConverterUtils {
  private static final String OR_REGEX_DELIMITER = "|";
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.007);

  private final CustomModsecRuleConverter customModsecRuleConverter;

  static final String EMPTY_STRING = "";

  @Inject
  public ModsecBlobConverterUtils(CustomModsecRuleConverter customModsecRuleConverter) {
    this.customModsecRuleConverter = customModsecRuleConverter;
  }

  String convertDataTypeToModsecRule(
      String ruleIdentifier,
      Clause urlRegexesClauseWrapper,
      Collection<Clause> ANDClausesList,
      String message,
      String logMessage,
      AtomicLong modsecIdAssignment) {
    try {
      List<Clause> clauses = new ArrayList<>();
      clauses.add(urlRegexesClauseWrapper);
      clauses.addAll(ANDClausesList);
      return customModsecRuleConverter.getJNIValidatedModsecRuleWithCustomLogMsg(
          modsecIdAssignment.getAndIncrement(), ruleIdentifier, message, clauses, logMessage);
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.warn(
            "Cannot convert datatypes of rateLimitingRule with id {} into modsec rule",
            ruleIdentifier,
            e);
      }
      return EMPTY_STRING;
    }
  }

  Clause buildUrlRegexClause(Collection<String> urlRegexes) {
    String combinedRegex = String.join(OR_REGEX_DELIMITER, urlRegexes);
    validateRegex(combinedRegex);

    return Clause.newBuilder()
        .setMatchExpression(
            MatchExpression.newBuilder()
                .setMatchKey(MatchKey.MATCH_KEY_URL)
                .setMatchOperator(
                    ai.traceable.customsignature.config.service.v1.MatchOperator
                        .MATCH_OPERATOR_MATCHES_REGEX)
                .setMatchValue(combinedRegex)
                .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST))
        .build();
  }

  @Deprecated
  Clause buildDeprecatedKeyValueClause(KeyValueCondition keyValueCondition) {

    KeyValueCondition.Type type = keyValueCondition.getType();
    boolean hasKeyCondition = keyValueCondition.hasKeyCondition();
    boolean hasValueCondition = keyValueCondition.hasValueCondition();
    KeyValueCondition.MatchOperator keyOperator =
        hasKeyCondition ? keyValueCondition.getKeyCondition().getOperator() : null;
    KeyValueCondition.MatchOperator valueOperator =
        hasValueCondition ? keyValueCondition.getValueCondition().getOperator() : null;
    String value = hasValueCondition ? keyValueCondition.getValueCondition().getValue() : null;
    String key = hasKeyCondition ? keyValueCondition.getKeyCondition().getValue() : null;
    return buildConditionalKeyValueClause(
        type, hasKeyCondition, hasValueCondition, keyOperator, valueOperator, key, value);
  }

  Clause buildKeyValueClause(KeyValueCondition keyValueCondition) {
    KeyValueCondition.StaticValueCondition staticValueCondition =
        keyValueCondition.getStaticValueCondition();
    KeyValueCondition.Type type = staticValueCondition.getKeyCondition().getKeyType();
    boolean hasKeyCondition = staticValueCondition.getKeyCondition().hasKeyMatchOperatorCondition();

    boolean hasValueCondition = staticValueCondition.hasValueMatchOperatorCondition();

    KeyValueCondition.MatchOperator keyOperator =
        hasKeyCondition
            ? staticValueCondition.getKeyCondition().getKeyMatchOperatorCondition().getOperator()
            : null;
    KeyValueCondition.MatchOperator valueOperator =
        hasValueCondition
            ? staticValueCondition.getValueMatchOperatorCondition().getOperator()
            : null;
    String value =
        hasValueCondition
            ? staticValueCondition.getValueMatchOperatorCondition().getValue().getStringValue()
            : null;
    String key =
        hasKeyCondition
            ? staticValueCondition
                .getKeyCondition()
                .getKeyMatchOperatorCondition()
                .getValue()
                .getStringValue()
            : null;
    return buildConditionalKeyValueClause(
        type, hasKeyCondition, hasValueCondition, keyOperator, valueOperator, key, value);
  }

  Clause buildConditionalKeyValueClause(
      Type type,
      boolean hasKeyCondition,
      boolean hasValueCondition,
      MatchOperator keyOperator,
      MatchOperator valueOperator,
      String key,
      String value) {
    ClauseDetails extractedClauseDetails = convertType(type);
    if (hasKeyCondition && hasValueCondition) {
      if (extractedClauseDetails.getKeyValueTagOptional().isPresent()) {
        validateRegex(key);
        validateRegex(value);

        return Clause.newBuilder()
            .setKeyValueExpression(
                KeyValueExpression.newBuilder()
                    .setTag(extractedClauseDetails.getKeyValueTagOptional().get())
                    .setMatchCategory(extractedClauseDetails.getCategory())
                    .setMatchKey(key)
                    .setKeyMatchOperator(convertOperator(keyOperator))
                    .setMatchValue(value)
                    .setValueMatchOperator(convertOperator(valueOperator)))
            .build();
      }
    } else if (hasValueCondition) {
      // Check if formed regex are valid
      validateRegex(value);

      return Clause.newBuilder()
          .setMatchExpression(
              MatchExpression.newBuilder()
                  .setMatchCategory(extractedClauseDetails.getCategory())
                  .setMatchKey(extractedClauseDetails.getValueType())
                  .setMatchOperator(convertOperator(valueOperator))
                  .setMatchValue(value))
          .build();
    } else if (hasKeyCondition && extractedClauseDetails.getKeyTypeOptional().isPresent()) {
      // Check if formed regex are valid
      validateRegex(key);

      return Clause.newBuilder()
          .setMatchExpression(
              MatchExpression.newBuilder()
                  .setMatchCategory(extractedClauseDetails.getCategory())
                  .setMatchKey(extractedClauseDetails.getKeyTypeOptional().get())
                  .setMatchOperator(convertOperator(keyOperator))
                  .setMatchValue(key))
          .build();
    }
    throw new UnsupportedOperationException(
        String.format("Cannot convert key-value-condition into modsec rule"));
  }

  static void validateRegex(String combinedRegex) {
    Status validationStatus = RegexValidator.validateRegex(combinedRegex);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
  }

  private ClauseDetails convertType(Type type) {
    switch (type) {
      case TYPE_URL:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_URL,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_HOST:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_HOST,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_HTTP_METHOD:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_HTTP_METHOD,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_USER_AGENT:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_USER_AGENT,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_STATUS_CODE:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_STATUS_CODE,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case TYPE_REQUEST_BODY:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_BODY,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_RESPONSE_BODY:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_BODY,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case TYPE_REQUEST_HEADER:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_HEADER_NAME),
            MatchKey.MATCH_KEY_HEADER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_HEADER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_RESPONSE_HEADER:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_HEADER_NAME),
            MatchKey.MATCH_KEY_HEADER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_HEADER),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case TYPE_REQUEST_COOKIE:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_COOKIE_NAME),
            MatchKey.MATCH_KEY_COOKIE_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_COOKIE),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_QUERY_PARAMETER:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_QUERY_PARAMETER_NAME),
            MatchKey.MATCH_KEY_QUERY_PARAMETER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_QUERY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_REQUEST_BODY_PARAMETER:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_BODY_PARAMETER_NAME),
            MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_RESPONSE_BODY_PARAMETER:
        return new ClauseDetails(
            Optional.of(MatchKey.MATCH_KEY_BODY_PARAMETER_NAME),
            MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case TYPE_TAG:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_UNSPECIFIED,
            Optional.of(KeyValueTag.KEY_VALUE_TAG_PARAMETER),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_RESPONSE_BODY_SIZE:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_BODY_SIZE,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case TYPE_REQUEST_BODY_SIZE:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_BODY_SIZE,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_QUERY_PARAMS_COUNT:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_REQUEST_HEADERS_COUNT:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_HEADERS_COUNT,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      case TYPE_RESPONSE_HEADERS_COUNT:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_HEADERS_COUNT,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_RESPONSE);
      case TYPE_REQUEST_COOKIES_COUNT:
        return new ClauseDetails(
            Optional.empty(),
            MatchKey.MATCH_KEY_COOKIES_COUNT,
            Optional.empty(),
            MatchCategory.MATCH_CATEGORY_REQUEST);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert a condition of type:%s into modsec rule", type));
    }
  }

  private static final EnumMap<
          MatchOperator, ai.traceable.customsignature.config.service.v1.MatchOperator>
      matchOperatorMapping =
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
                  MatchOperator.MATCH_OPERATOR_CONTAINS,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_CONTAINS,
                  MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_NOT_CONTAIN,
                  MatchOperator.MATCH_OPERATOR_GREATER_THAN,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_GREATER_THAN,
                  MatchOperator.MATCH_OPERATOR_LESS_THAN,
                  ai.traceable.customsignature.config.service.v1.MatchOperator
                      .MATCH_OPERATOR_LESS_THAN));

  private static ai.traceable.customsignature.config.service.v1.MatchOperator convertOperator(
      MatchOperator matchOperator) {
    return Optional.ofNullable(matchOperatorMapping.get(matchOperator))
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format(
                        "Cannot convert a operator of type:%s into modsec rule", matchOperator)));
  }

  @Value
  static class ClauseDetails {
    Optional<MatchKey> keyTypeOptional;
    MatchKey valueType;
    Optional<KeyValueTag> KeyValueTagOptional;
    MatchCategory category;
  }
}
