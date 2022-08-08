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
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.CustomParsingRule;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.CustomParsingRule.JwtParser;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.CustomParsingRule.NoOpParser;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule.TransformedExternalUserAttributionRule.ParsingTarget;
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
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExternalUserAttributionRuleTranslator {
  private static final List<String> RESPONSE_BODY_KEYS =
      List.of("http.response.body", "rpc.response.body");
  private static final String COOKIE_HEADER_KEY = "http.request.header.cookie";
  private static final List<String> AUTH_HEADER_KEYS =
      List.of("http.request.header.authorization", "rpc.request.metadata.authorization");
  private static final String URL_KEY = "http.url";
  private static final List<String> REQUEST_HEADER_KEY_FORMAT_STRINGS =
      List.of("http.request.header.%s", "rpc.request.metadata.%s");
  private final Map<DefaultParsingRuleKey, List<CustomParsingRule>> defaultParsingRules;

  @Inject
  public ExternalUserAttributionRuleTranslator(
      ExternalUserAttributionConfigServiceConfig externalUserAttributionConfigServiceConfig) {
    Config rulesConfig = externalUserAttributionConfigServiceConfig.getParsingRulesConfig();
    defaultParsingRules =
        Stream.of(
                DefaultParsingRuleKey.BASIC_AUTHENTICATION,
                DefaultParsingRuleKey.JWT_HEADER,
                DefaultParsingRuleKey.JWT_COOKIE)
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    dataCase -> buildCustomParsingRules(rulesConfig, dataCase.toString())));
  }

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
        .map(externalRuleBuilder -> translateRuleScope(externalRuleBuilder, rule.getScope()))
        .map(Builder::build);
  }

  private ExternalUserAttributionRule.Builder translateRuleScope(
      ExternalUserAttributionRule.Builder builder, UserAttributionRuleScope scope) {
    ExternalUserAttributionRule.SpanFilter.Builder spanFilterBuilder =
        ExternalUserAttributionRule.SpanFilter.newBuilder();

    scope.getCustomScope().getUrlScopesList().stream()
        .map(UserAttributionRuleScope.UrlScope::getUrlMatchRegex)
        .forEach(
            urlRegex ->
                spanFilterBuilder.addRequiredMatchingAttributes(
                    ExternalUserAttributionRule.AttributePredicate.newBuilder()
                        .setNamePredicate(
                            buildStringPredicate(
                                ExternalUserAttributionRule.Operator.OPERATOR_EQUALS, URL_KEY))
                        .setValuePredicate(
                            buildStringPredicate(
                                ExternalUserAttributionRule.Operator.OPERATOR_MATCHES_REGEX,
                                urlRegex))));

    if (spanFilterBuilder.getRequiredMatchingAttributesCount() > 0) {
      builder.setSpanFilter(spanFilterBuilder);
    }

    return builder;
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
                            .setType(TYPE_AUTHHEADER)
                            .addAllAttributeValueParsingRules(
                                defaultParsingRules.getOrDefault(
                                    DefaultParsingRuleKey.BASIC_AUTHENTICATION, List.of())))
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

    boolean isCookieRule =
        data.getJwtLocation().getLocationCase() == HeaderLocation.LocationCase.COOKIE_NAME;
    if (data.getJwtLocation().getParsingTarget().hasRegexCaptureGroup()) {
      CustomParsingRule.Builder builder =
          CustomParsingRule.newBuilder()
              .setParsingTarget(
                  buildRegexCaptureGroup(
                      data.getJwtLocation().getParsingTarget().getRegexCaptureGroup()));
      if (isCookieRule) {
        ruleBuilder.addAttributeValueParsingRules(
            builder.setCookieParser(CustomParsingRule.CookieParser.getDefaultInstance()));
      } else {
        ruleBuilder.addAttributeValueParsingRules(
            builder.setJwtParser(JwtParser.getDefaultInstance()));
      }
    }

    DefaultParsingRuleKey defaultRulesKey =
        isCookieRule ? DefaultParsingRuleKey.JWT_COOKIE : DefaultParsingRuleKey.JWT_HEADER;
    defaultParsingRules
        .getOrDefault(defaultRulesKey, List.of())
        .forEach(ruleBuilder::addAttributeValueParsingRules);

    this.getStringIfSet(data.getJwtLocation().getCookieName())
        .ifPresent(ruleBuilder::setCookieName);
    ruleBuilder.setEncoding(ENCODING_JWT);

    ruleBuilder.addIdClaims(data.getUserIdClaim());
    this.getPathIfSet(data.getUserIdLocation()).ifPresent(ruleBuilder::addIdPaths);
    if (data.getUserIdLocation().getParsingTarget().hasRegexCaptureGroup()) {
      ruleBuilder.addIdParsingRules(
          CustomParsingRule.newBuilder()
              .setParsingTarget(
                  buildRegexCaptureGroup(
                      data.getUserIdLocation().getParsingTarget().getRegexCaptureGroup()))
              .setNoOpParser(NoOpParser.getDefaultInstance()));
    }

    this.getStringIfSet(data.getRoleClaim()).ifPresent(ruleBuilder::addRoleClaims);
    this.getPathIfSet(data.getRoleLocation()).ifPresent(ruleBuilder::addRolePaths);
    if (data.getRoleLocation().getParsingTarget().hasRegexCaptureGroup()) {
      ruleBuilder.addRoleParsingRules(
          CustomParsingRule.newBuilder()
              .setParsingTarget(
                  buildRegexCaptureGroup(
                      data.getRoleLocation().getParsingTarget().getRegexCaptureGroup()))
              .setNoOpParser(NoOpParser.getDefaultInstance()));
    }

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
              if (data.getUserIdLocation().getParsingTarget().hasRegexCaptureGroup()) {
                ruleBuilder.addIdParsingRules(
                    CustomParsingRule.newBuilder()
                        .setParsingTarget(
                            buildRegexCaptureGroup(
                                data.getUserIdLocation().getParsingTarget().getRegexCaptureGroup()))
                        .setNoOpParser(NoOpParser.getDefaultInstance()));
              }

              this.getPathIfSet(data.getRoleLocation()).ifPresent(ruleBuilder::addRolePaths);
              if (data.getRoleLocation().getParsingTarget().hasRegexCaptureGroup()) {
                ruleBuilder.addRoleParsingRules(
                    CustomParsingRule.newBuilder()
                        .setParsingTarget(
                            buildRegexCaptureGroup(
                                data.getRoleLocation().getParsingTarget().getRegexCaptureGroup()))
                        .setNoOpParser(NoOpParser.getDefaultInstance()));
              }

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
                TransformedExternalUserAttributionRule.newBuilder()
                    .setType(type)
                    .setAttributeKey(headerKey))
        .map(
            transformedRuleBuilder -> {
              if (headerLocation.getParsingTarget().hasRegexCaptureGroup()
                  && (type == TYPE_ID || type == TYPE_ROLE)) {
                transformedRuleBuilder.addAttributeValueParsingRules(
                    CustomParsingRule.newBuilder()
                        .setParsingTarget(
                            buildRegexCaptureGroup(
                                headerLocation.getParsingTarget().getRegexCaptureGroup()))
                        .setNoOpParser(NoOpParser.getDefaultInstance()));
              }
              return transformedRuleBuilder.build();
            })
        .map(
            transformedRule ->
                ExternalUserAttributionRule.newBuilder()
                    .setTransformedExternalUserAttributionRule(transformedRule)
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

  private List<CustomParsingRule> buildCustomParsingRules(Config config, String type) {
    return config.hasPath(type)
        ? config.getConfigList(type).stream()
            .map(this::buildCustomParsingRuleFromConfig)
            .collect(Collectors.toList())
        : List.of();
  }

  @SneakyThrows
  private CustomParsingRule buildCustomParsingRuleFromConfig(Config ruleConfig) {
    CustomParsingRule.Builder builder = CustomParsingRule.newBuilder();
    JsonFormat.parser().merge(ruleConfig.root().render(), builder);
    return builder.build();
  }

  private Optional<String> getStringIfSet(String value) {
    return Optional.ofNullable(value).filter(Predicate.not(String::isEmpty));
  }

  private Optional<String> getPathIfSet(EncodedLocation location) {
    return this.getStringIfSet(location.getJsonPath());
  }

  private ParsingTarget buildRegexCaptureGroup(String regex) {
    return ParsingTarget.newBuilder().setRegexCaptureGroup(regex).build();
  }

  private ExternalUserAttributionRule.StringPredicate buildStringPredicate(
      ExternalUserAttributionRule.Operator operator, String value) {
    return ExternalUserAttributionRule.StringPredicate.newBuilder()
        .setValue(value)
        .setOperator(operator)
        .build();
  }

  private enum DefaultParsingRuleKey {
    BASIC_AUTHENTICATION,
    JWT_HEADER,
    JWT_COOKIE;
  }
}
