package ai.traceable.external.userattribution.config.service;

import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Encoding.ENCODING_JWT;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_AUTHHEADER;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_COOKIE;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_ID;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_JSON;
import static ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.Type.TYPE_ROLE;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule;
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
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExternalUserAttributionRuleTranslator {
  private static final String RESPONSE_BODY_KEY = "http.response.body";
  private static final String COOKIE_HEADER_KEY = "http.request.header.cookie";
  private static final String AUTH_HEADER_KEY = "http.request.header.authorization";
  private static final String URL_KEY = "http.url";
  private static final String REQUEST_HEADER_KEY_FORMAT_STRING = "http.request.header.%s";

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
    try {
      switch (rule.getData().getDataCase()) {
        case CUSTOM_DATA:
          return Stream.of(this.translateCustom(rule.getData().getCustomData()));
        case BASIC_AUTHENTICATION_DATA:
          return Stream.of(this.translateBasicAuth(rule.getData().getBasicAuthenticationData()));
        case JWT_DATA:
          return Stream.of(this.translateUserJwt(rule.getData().getJwtData()));
        case RESPONSE_BODY_DATA:
          return Stream.of(this.translateUserResponseBody(rule.getData().getResponseBodyData()));
        case REQUEST_HEADER_DATA:
          return this.translateUserRequestHeader(rule.getData().getRequestHeaderData()).stream();
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

  private ExternalUserAttributionRule translateBasicAuth(
      BasicAuthenticationUserAttributionRuleData data) {
    return ExternalUserAttributionRule.newBuilder()
        .setTransformedExternalUserAttributionRule(
            TransformedExternalUserAttributionRule.newBuilder()
                .setAttributeKey(
                    this.getHeaderAttributeKeyIfSet(data.getLocation()).orElse(AUTH_HEADER_KEY))
                .setType(TYPE_AUTHHEADER))
        .build();
  }

  private ExternalUserAttributionRule translateUserJwt(JwtUserAttributionRuleData data) {
    TransformedExternalUserAttributionRule.Builder ruleBuilder =
        TransformedExternalUserAttributionRule.newBuilder();

    ruleBuilder.setAttributeKey(
        this.getAttributeKeyIfSet(data.getJwtLocation()).orElse(AUTH_HEADER_KEY));
    ruleBuilder.setType(
        this.getTypeFromHeaderLocationIfSet(data.getJwtLocation()).orElse(TYPE_AUTHHEADER));
    ruleBuilder.setEncoding(ENCODING_JWT);
    this.getStringIfSet(data.getJwtLocation().getCookieName())
        .ifPresent(ruleBuilder::setCookieName);

    ruleBuilder.addIdClaims(data.getUserIdClaim());
    this.getPathIfSet(data.getUserIdLocation()).ifPresent(ruleBuilder::addIdPaths);

    this.getStringIfSet(data.getRoleClaim()).ifPresent(ruleBuilder::addRoleClaims);
    this.getPathIfSet(data.getRoleLocation()).ifPresent(ruleBuilder::addRolePaths);

    return ExternalUserAttributionRule.newBuilder()
        .setTransformedExternalUserAttributionRule(ruleBuilder)
        .build();
  }

  private ExternalUserAttributionRule translateUserResponseBody(
      ResponseBodyUserAttributionRuleData data) {
    TransformedExternalUserAttributionRule.Builder ruleBuilder =
        TransformedExternalUserAttributionRule.newBuilder()
            .setAttributeKey(RESPONSE_BODY_KEY)
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
  }

  private List<ExternalUserAttributionRule> translateUserRequestHeader(
      RequestHeaderUserAttributionRuleData data) {
    return Stream.concat(
            this.getHeaderRuleIfSet(data.getUserIdLocation(), TYPE_ID).stream(),
            this.getHeaderRuleIfSet(data.getRoleLocation(), TYPE_ROLE).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<ExternalUserAttributionRule> getHeaderRuleIfSet(
      HeaderLocation headerLocation, Type type) {
    return this.getHeaderAttributeKeyIfSet(headerLocation)
        .map(
            headerKey ->
                ExternalUserAttributionRule.newBuilder()
                    .setTransformedExternalUserAttributionRule(
                        TransformedExternalUserAttributionRule.newBuilder()
                            .setType(type)
                            .setAttributeKey(headerKey))
                    .build());
  }

  private Optional<String> getAttributeKeyIfSet(HeaderLocation headerLocation) {
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        return this.getHeaderAttributeKeyIfSet(headerLocation);
      case COOKIE_NAME:
        return Optional.of(COOKIE_HEADER_KEY);
      case LOCATION_NOT_SET:
      default:
        return Optional.empty();
    }
  }

  private Optional<String> getHeaderAttributeKeyIfSet(HeaderLocation headerLocation) {
    return this.getStringIfSet(headerLocation.getHeaderName())
        .map(String::toLowerCase)
        .map(headerName -> String.format(REQUEST_HEADER_KEY_FORMAT_STRING, headerName));
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
