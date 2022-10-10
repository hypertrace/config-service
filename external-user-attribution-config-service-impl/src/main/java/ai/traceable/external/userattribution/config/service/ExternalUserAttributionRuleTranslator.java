package ai.traceable.external.userattribution.config.service;

import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Encoding.ENCODING_JWT;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_AUTHHEADER;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_COOKIE;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_ID;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_JSON;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_ROLE;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.Builder;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.RawExternalUserAttributionRule;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Condition;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRules;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.BasicAuthenticationUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.EnvironmentScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.UrlScope;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExternalUserAttributionRuleTranslator {
  private static final List<String> RESPONSE_BODY_KEYS =
      List.of("http.response.body", "rpc.response.body");
  private static final String COOKIE_HEADER_KEY = "http.request.header.cookie";
  private static final List<String> AUTH_HEADER_KEYS =
      List.of("http.request.header.authorization", "rpc.request.metadata.authorization");
  private static final String URL_KEY = "http.url";
  private static final String ENVIRONMENT_NAME_KEY = "deployment.environment";
  private static final List<String> REQUEST_HEADER_KEY_FORMAT_STRINGS =
      List.of("http.request.header.%s", "rpc.request.metadata.%s");

  ExternalUserAttributionRules translateRules(List<UserAttributionRule> rules) {
    return rules.stream()
        .flatMap(this::translateRule)
        .collect(Collectors.collectingAndThen(Collectors.toUnmodifiableList(), this::buildRules));
  }

  private ExternalUserAttributionRules buildRules(List<ExternalUserAttributionRule> rules) {
    return ExternalUserAttributionRules.newBuilder()
        .addAllExternalUserAttributionRule(rules)
        .build();
  }

  private Stream<ExternalUserAttributionRule> translateRule(UserAttributionRule rule) {
    return this.translateRuleContent(rule)
        .map(ExternalUserAttributionRule::toBuilder)
        .map(externalRuleBuilder -> externalRuleBuilder.setRuleId(rule.getId()))
        .map(externalRuleBuilder -> translateRuleScope(externalRuleBuilder, rule))
        .map(Builder::build);
  }

  private ExternalUserAttributionRule.Builder translateRuleScope(
      ExternalUserAttributionRule.Builder builder, UserAttributionRule rule) {
    if (rule.getData().hasCustomData()) { // no condition added for a custom Yaml rule
      return builder;
    }

    List<String> urlScopes =
        rule.getScope().getCustomScope().getUrlScopesList().stream()
            .map(UrlScope::getUrlMatchRegex)
            .collect(Collectors.toUnmodifiableList());
    getConditionRegex(urlScopes)
        .ifPresent(
            urlConditionRegex ->
                builder
                    .getTransformedExternalUserAttributionRuleBuilder()
                    .addConditions(
                        Condition.newBuilder()
                            .setKey(URL_KEY)
                            .setRegex(urlConditionRegex)
                            .build()));

    List<String> environmentScopes =
        rule.getScope().getCustomScope().getEnvironmentScopesList().stream()
            .map(EnvironmentScope::getEnvironmentName)
            .collect(Collectors.toUnmodifiableList());
    getConditionRegex(environmentScopes)
        .ifPresent(
            environmentConditionRegex ->
                builder
                    .getTransformedExternalUserAttributionRuleBuilder()
                    .addConditions(
                        Condition.newBuilder()
                            .setKey(ENVIRONMENT_NAME_KEY)
                            .setRegex(environmentConditionRegex)
                            .build()));
    return builder;
  }

  private Optional<String> getConditionRegex(List<String> allowedValues) {
    if (allowedValues.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(String.join("|", allowedValues));
  }

  private Stream<ExternalUserAttributionRule> translateRuleContent(UserAttributionRule rule) {
    try {
      switch (rule.getData().getDataCase()) {
        case CUSTOM_DATA:
          return Stream.of(this.translateCustom(rule.getData().getCustomData()));
        case BASIC_AUTHENTICATION_DATA:
          return this.translateBasicAuth(rule.getData().getBasicAuthenticationData());
        case JWT_DATA:
          return this.translateUserJwt(rule.getData().getJwtData());
        case RESPONSE_BODY_DATA:
          return this.translateUserResponseBody(rule.getData().getResponseBodyData());
        case REQUEST_HEADER_DATA:
          return this.translateUserRequestHeader(rule.getData().getRequestHeaderData());
        case CUSTOM_TOKEN_DATA:
        case CUSTOM_JSON_DATA:
          return Stream.empty();
        case DATA_NOT_SET:
        default:
          log.error("Unrecognized rule: {}", rule);
          return Stream.empty();
      }
    } catch (Exception exception) {
      log.error("Error translating rule", exception);
      return Stream.empty();
    }
  }

  private ExternalUserAttributionRule translateCustom(CustomUserAttributionRuleData data) {
    return ExternalUserAttributionRule.newBuilder()
        .setRawExternalUserAttributionRule(
            RawExternalUserAttributionRule.newBuilder().setYaml(data.getYaml()))
        .build();
  }

  private Stream<ExternalUserAttributionRule> translateBasicAuth(
      BasicAuthenticationUserAttributionRuleData data) {
    return this.getHeaderAttributeKeysIfSet(data.getLocation()).orElse(AUTH_HEADER_KEYS).stream()
        .map(
            key ->
                ExternalUserAttributionRule.newBuilder()
                    .setTransformedExternalUserAttributionRule(
                        TransformedExternalUserAttributionRule.newBuilder()
                            .setAttributeKey(key)
                            .setType(TYPE_AUTHHEADER))
                    .build());
  }

  private Stream<ExternalUserAttributionRule> translateUserJwt(JwtUserAttributionRuleData data) {
    return this.getAttributeKeysIfSet(data.getJwtLocation()).orElse(AUTH_HEADER_KEYS).stream()
        .map(attributeKey -> this.translateUserJwtForAttributeKey(data, attributeKey));
  }

  private ExternalUserAttributionRule translateUserJwtForAttributeKey(
      JwtUserAttributionRuleData data, String attributeKey) {
    TransformedExternalUserAttributionRule.Builder ruleBuilder =
        TransformedExternalUserAttributionRule.newBuilder();

    ruleBuilder.setAttributeKey(attributeKey);
    ruleBuilder.setType(
        this.getTypeFromHeaderLocationIfSet(data.getJwtLocation()).orElse(TYPE_AUTHHEADER));
    this.getStringIfSet(data.getJwtLocation().getCookieName())
        .ifPresent(ruleBuilder::setCookieName);
    ruleBuilder.setEncoding(ENCODING_JWT);
    ruleBuilder.addIdClaims(data.getUserIdClaim());
    this.getPathIfSet(data.getUserIdLocation()).ifPresent(ruleBuilder::addIdPaths);

    this.getStringIfSet(data.getRoleClaim()).ifPresent(ruleBuilder::addRoleClaims);
    this.getPathIfSet(data.getRoleLocation()).ifPresent(ruleBuilder::addRolePaths);

    return ExternalUserAttributionRule.newBuilder()
        .setTransformedExternalUserAttributionRule(ruleBuilder)
        .build();
  }

  private Stream<ExternalUserAttributionRule> translateUserResponseBody(
      ResponseBodyUserAttributionRuleData data) {
    return RESPONSE_BODY_KEYS.stream()
        .map(
            key -> {
              TransformedExternalUserAttributionRule.Builder ruleBuilder =
                  TransformedExternalUserAttributionRule.newBuilder()
                      .setAttributeKey(key)
                      .setType(TYPE_JSON)
                      .addConditions(
                          Condition.newBuilder()
                              .setKey(URL_KEY)
                              .setRegex(data.getCondition().getUrlMatchRegex()))
                      .addIdPaths(this.getPathIfSet(data.getUserIdLocation()).orElseThrow());

              this.getPathIfSet(data.getRoleLocation()).ifPresent(ruleBuilder::addRolePaths);

              return ExternalUserAttributionRule.newBuilder()
                  .setTransformedExternalUserAttributionRule(ruleBuilder)
                  .build();
            });
  }

  private Stream<ExternalUserAttributionRule> translateUserRequestHeader(
      RequestHeaderUserAttributionRuleData data) {
    return Stream.concat(
        this.getHeaderRuleIfSet(data.getUserIdLocation(), TYPE_ID),
        this.getHeaderRuleIfSet(data.getRoleLocation(), TYPE_ROLE));
  }

  private Stream<ExternalUserAttributionRule> getHeaderRuleIfSet(
      HeaderLocation headerLocation, Type type) {
    return this.getHeaderAttributeKeysIfSet(headerLocation).orElse(Collections.emptyList()).stream()
        .map(
            headerKey ->
                ExternalUserAttributionRule.newBuilder()
                    .setTransformedExternalUserAttributionRule(
                        TransformedExternalUserAttributionRule.newBuilder()
                            .setType(type)
                            .setAttributeKey(headerKey))
                    .build());
  }

  private Optional<List<String>> getAttributeKeysIfSet(HeaderLocation headerLocation) {
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        return this.getHeaderAttributeKeysIfSet(headerLocation);
      case COOKIE_NAME:
        return Optional.of(List.of(COOKIE_HEADER_KEY));
      case LOCATION_NOT_SET:
      default:
        return Optional.empty();
    }
  }

  private Optional<List<String>> getHeaderAttributeKeysIfSet(HeaderLocation headerLocation) {
    return this.getStringIfSet(headerLocation.getHeaderName())
        .map(String::toLowerCase)
        .map(
            headerName ->
                REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
                    .map(formatString -> String.format(formatString, headerName))
                    .collect(Collectors.toUnmodifiableList()));
  }

  private Optional<Type> getTypeFromHeaderLocationIfSet(HeaderLocation headerLocation) {
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        return Optional.of(TYPE_AUTHHEADER);
      case COOKIE_NAME:
        return Optional.of(TYPE_COOKIE);
      case LOCATION_NOT_SET:
      default:
        return Optional.empty();
    }
  }

  private Optional<String> getStringIfSet(String value) {
    return Optional.ofNullable(value).filter(Predicate.not(String::isEmpty));
  }

  private Optional<String> getPathIfSet(EncodedLocation location) {
    return this.getStringIfSet(location.getJsonPath());
  }
}
