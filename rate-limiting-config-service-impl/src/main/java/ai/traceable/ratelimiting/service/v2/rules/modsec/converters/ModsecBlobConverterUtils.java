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
  private final CustomModsecRuleConverter customModsecRuleConverter;
  static final String EMPTY_STRING = "";

  @Inject
  public ModsecBlobConverterUtils(CustomModsecRuleConverter customModsecRuleConverter) {
    this.customModsecRuleConverter = customModsecRuleConverter;
  }

  String convertToModsecRule(
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

      return customModsecRuleConverter.getValidatedModsecRuleWithCustomLogMsg(
          modsecIdAssignment.getAndIncrement(), ruleIdentifier, message, clauses, logMessage);
    } catch (Exception e) {
      log.warn("Cannot convert rateLimitingRule with id {} into modsec rule", ruleIdentifier, e);
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

  Clause buildKeyValueClause(KeyValueCondition keyValueCondition) {
    ClauseDetails extractedClauseDetails = convertType(keyValueCondition.getType());
    if (keyValueCondition.hasKeyCondition() && keyValueCondition.hasValueCondition()) {
      if (extractedClauseDetails.getKeyValueTagOptional().isPresent()) {
        validateRegex(keyValueCondition.getKeyCondition().getValue());
        validateRegex(keyValueCondition.getValueCondition().getValue());

        return Clause.newBuilder()
            .setKeyValueExpression(
                KeyValueExpression.newBuilder()
                    .setTag(extractedClauseDetails.getKeyValueTagOptional().get())
                    .setMatchCategory(extractedClauseDetails.getCategory())
                    .setMatchKey(keyValueCondition.getKeyCondition().getValue())
                    .setKeyMatchOperator(
                        convertOperator(keyValueCondition.getKeyCondition().getOperator()))
                    .setMatchValue(keyValueCondition.getValueCondition().getValue())
                    .setValueMatchOperator(
                        convertOperator(keyValueCondition.getValueCondition().getOperator())))
            .build();
      }
    } else if (keyValueCondition.hasValueCondition()) {
      // Check if formed regex are valid
      validateRegex(keyValueCondition.getValueCondition().getValue());

      return Clause.newBuilder()
          .setMatchExpression(
              MatchExpression.newBuilder()
                  .setMatchCategory(extractedClauseDetails.getCategory())
                  .setMatchKey(extractedClauseDetails.getValueType())
                  .setMatchOperator(
                      convertOperator(keyValueCondition.getValueCondition().getOperator()))
                  .setMatchValue(keyValueCondition.getValueCondition().getValue()))
          .build();
    } else if (extractedClauseDetails.getKeyTypeOptional().isPresent()) {
      // Check if formed regex are valid
      validateRegex(keyValueCondition.getKeyCondition().getValue());

      return Clause.newBuilder()
          .setMatchExpression(
              MatchExpression.newBuilder()
                  .setMatchCategory(extractedClauseDetails.getCategory())
                  .setMatchKey(extractedClauseDetails.getKeyTypeOptional().get())
                  .setMatchOperator(
                      convertOperator(keyValueCondition.getKeyCondition().getOperator()))
                  .setMatchValue(keyValueCondition.getKeyCondition().getValue()))
          .build();
    }
    throw new UnsupportedOperationException(
        String.format(
            "Cannot convert key-value-condition - %s, into modsec rule", keyValueCondition));
  }

  static void validateRegex(String combinedRegex) {
    Status validationStatus = RegexValidator.validate(combinedRegex);
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
                      .MATCH_OPERATOR_NOT_CONTAIN));

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
