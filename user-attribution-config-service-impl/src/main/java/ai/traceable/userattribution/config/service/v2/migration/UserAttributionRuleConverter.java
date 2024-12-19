package ai.traceable.userattribution.config.service.v2.migration;

import static ai.traceable.userattribution.config.service.v2.KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS;
import static ai.traceable.userattribution.config.service.v2.Template.TEMPLATE_BASIC;
import static ai.traceable.userattribution.config.service.v2.Template.TEMPLATE_CUSTOM;
import static ai.traceable.userattribution.config.service.v2.Template.TEMPLATE_JWT;

import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.CustomProjection;
import ai.traceable.userattribution.config.service.v2.EnvironmentScope;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.LiteralValue;
import ai.traceable.userattribution.config.service.v2.LiteralValueProjection;
import ai.traceable.userattribution.config.service.v2.MatchCondition;
import ai.traceable.userattribution.config.service.v2.PayloadMatch;
import ai.traceable.userattribution.config.service.v2.Predicate;
import ai.traceable.userattribution.config.service.v2.RootRelativeProjection;
import ai.traceable.userattribution.config.service.v2.StringList;
import ai.traceable.userattribution.config.service.v2.UrlScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import ai.traceable.userattribution.config.service.v2.ValueMatchOperator;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class UserAttributionRuleConverter {
  static final String YAML_FORMAT_NOT_SUPPORTED =
      "Unsupported format - yaml, cannot be converted to new format";
  static final String BASIC_AUTH_TYPE = "Basic";
  static final String DEFAULT_BASIC_AUTHORIZATION_HEADER_NAME = "authorization";
  static final String DEFAULT_BASIC_AUTHORIZATION_REGEX_CAPTURE_GROUP = "(?i)Basic:? (.*)";
  static final String DEFAULT_BASIC_AUTHORIZATION_USERNAME_REGEX_CAPTURE_GROUP = "^([^:]+):?";
  private static final AttributeProjection.Builder DEFAULT_BASIC_AUTHORIZATION_ATTRIBUTE_BUILDER =
      createDefaultBasicAuthorizationAttributeBuilder();

  Optional<UserAttributionRule> convert(
      ai.traceable.userattribution.config.service.v1.UserAttributionRule legacyRule) {
    try {
      UserAttributionRule.Builder rule = UserAttributionRule.newBuilder();
      rule.setId(legacyRule.getId());
      rule.setRank(legacyRule.getRank());
      rule.setData(createUserAttributionRuleData(legacyRule));
      return Optional.of(rule.build());
    } catch (Exception ex) {
      if (ex instanceof LegacyUserAttributionRuleTranslationException
          && ex.getMessage().equals(YAML_FORMAT_NOT_SUPPORTED)) {
        log.debug("Unable to translate legacy rule to new rule format : {}", legacyRule, ex);
      } else {
        log.error("Unable to translate legacy rule to new rule format : {}", legacyRule, ex);
      }
      return Optional.empty();
    }
  }

  private UserAttributionRuleData createUserAttributionRuleData(
      ai.traceable.userattribution.config.service.v1.UserAttributionRule legacyRule)
      throws LegacyUserAttributionRuleTranslationException {
    UserAttributionRuleData.Builder builder = UserAttributionRuleData.newBuilder();
    builder.setName(legacyRule.getName());
    if (legacyRule.hasScope()) {
      updateScope(builder, legacyRule.getScope());
    }
    updateData(builder, legacyRule.getData());
    builder.setDisabled(legacyRule.getDisabled());
    return builder.build();
  }

  private void updateScope(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope scope) {
    if (scope.hasCustomScope()) {
      UserAttributionRuleScope.Builder scopeBuilder = UserAttributionRuleScope.newBuilder();
      List<String> environmentNames =
          scope.getCustomScope().getEnvironmentScopesList().stream()
              .map(
                  ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope
                          .EnvironmentScope
                      ::getEnvironmentName)
              .collect(Collectors.toUnmodifiableList());
      if (!environmentNames.isEmpty()) {
        scopeBuilder.setEnvironmentScope(
            EnvironmentScope.newBuilder()
                .setEnvironmentNames(StringList.newBuilder().addAllValues(environmentNames)));
      }
      List<String> urlRegexes =
          scope.getCustomScope().getUrlScopesList().stream()
              .map(
                  ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.UrlScope
                      ::getUrlMatchRegex)
              .collect(Collectors.toUnmodifiableList());
      if (!urlRegexes.isEmpty()) {
        scopeBuilder.setUrlScope(
            UrlScope.newBuilder()
                .setUrlMatchRegexes(StringList.newBuilder().addAllValues(urlRegexes)));
      }
      builder.setScope(scopeBuilder);
    }
  }

  private void updateData(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData data)
      throws LegacyUserAttributionRuleTranslationException {
    switch (data.getDataCase()) {
      case BASIC_AUTHENTICATION_DATA:
        convertBasicAuthenticationData(builder, data.getBasicAuthenticationData());
        break;
      case JWT_DATA:
        convertJwtData(builder, data.getJwtData());
        break;
      case REQUEST_HEADER_DATA:
        convertRequestHeaderData(builder, data.getRequestHeaderData());
        break;
      case RESPONSE_BODY_DATA:
        convertResponseBodyData(builder, data.getResponseBodyData());
        break;
      case CUSTOM_JSON_DATA:
        convertCustomJsonData(builder, data.getCustomJsonData());
        break;
      case CUSTOM_TOKEN_DATA:
        convertCustomToken(builder, data.getCustomTokenData());
        break;
      case CUSTOM_DATA:
        throw new LegacyUserAttributionRuleTranslationException(YAML_FORMAT_NOT_SUPPORTED);
      default:
        throw new LegacyUserAttributionRuleTranslationException(
            "Unsupported data case in new format");
    }
  }

  private void convertBasicAuthenticationData(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData
              .BasicAuthenticationUserAttributionRuleData
          basicAuthenticationRuleData)
      throws LegacyUserAttributionRuleTranslationException {
    builder.setTemplate(TEMPLATE_BASIC);
    UserAttributionTokenRule.Builder tokenRuleBuilder = UserAttributionTokenRule.newBuilder();
    AttributeProjection.Builder attributeProjectionBuilder =
        basicAuthenticationRuleData.hasLocation()
            ? createAttributeProjectionBuilder(basicAuthenticationRuleData.getLocation())
            : DEFAULT_BASIC_AUTHORIZATION_ATTRIBUTE_BUILDER;
    tokenRuleBuilder.setAttributeProjection(attributeProjectionBuilder);
    builder.setUserIdRule(tokenRuleBuilder);
  }

  private void convertJwtData(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData
              .JwtUserAttributionRuleData
          jwtRuleData)
      throws LegacyUserAttributionRuleTranslationException {
    builder.setTemplate(TEMPLATE_JWT);
    if (!jwtRuleData.hasJwtLocation()) {
      throw new LegacyUserAttributionRuleTranslationException(
          "unable to find root token rule in jwt rule");
    } else {
      AttributeProjection.Builder attributeProjectionBuilder =
          createAttributeProjectionBuilder(jwtRuleData.getJwtLocation());
      builder.setRootTokenRule(
          UserAttributionRootTokenRule.newBuilder()
              .setAttributeProjection(attributeProjectionBuilder));
    }
    if (!jwtRuleData.getUserIdClaim().isBlank()) {
      RootRelativeProjection.Builder rootRelativeProjectionBuilder =
          RootRelativeProjection.newBuilder();
      rootRelativeProjectionBuilder.addValueProjections(
          ValueProjection.newBuilder()
              .setJwtPayloadClaim(
                  ValueProjection.JwtPayloadClaimProjection.newBuilder()
                      .setClaimKey(jwtRuleData.getUserIdClaim())));
      if (jwtRuleData.hasUserIdLocation()) {
        rootRelativeProjectionBuilder.addAllValueProjections(
            getValueProjections(jwtRuleData.getUserIdLocation()));
      }
      builder.setUserIdRule(
          UserAttributionTokenRule.newBuilder()
              .setRootRelativeProjection(rootRelativeProjectionBuilder));
    }
    if (!jwtRuleData.getRoleClaim().isBlank()) {
      RootRelativeProjection.Builder rootRelativeProjectionBuilder =
          RootRelativeProjection.newBuilder();
      rootRelativeProjectionBuilder.addValueProjections(
          ValueProjection.newBuilder()
              .setJwtPayloadClaim(
                  ValueProjection.JwtPayloadClaimProjection.newBuilder()
                      .setClaimKey(jwtRuleData.getRoleClaim())));
      if (jwtRuleData.hasRoleLocation()) {
        rootRelativeProjectionBuilder.addAllValueProjections(
            getValueProjections(jwtRuleData.getRoleLocation()));
      }
      builder.setUserRoleRule(
          UserAttributionTokenRule.newBuilder()
              .setRootRelativeProjection(rootRelativeProjectionBuilder));
    }
    if (jwtRuleData.hasAuthentication()) {
      builder.setAuthTypeRule(
          getLiteralValueProjectionBuilder(jwtRuleData.getAuthentication().getType()));
    }
  }

  private void convertRequestHeaderData(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData
              .RequestHeaderUserAttributionRuleData
          requestHeaderRuleData)
      throws LegacyUserAttributionRuleTranslationException {
    builder.setTemplate(TEMPLATE_CUSTOM);
    if (requestHeaderRuleData.hasUserIdLocation()) {
      AttributeProjection.Builder attributeProjectionBuilder =
          createAttributeProjectionBuilder(requestHeaderRuleData.getUserIdLocation());
      builder.setUserIdRule(
          UserAttributionTokenRule.newBuilder().setAttributeProjection(attributeProjectionBuilder));
    }
    if (requestHeaderRuleData.hasRoleLocation()) {
      AttributeProjection.Builder attributeProjectionBuilder =
          createAttributeProjectionBuilder(requestHeaderRuleData.getRoleLocation());
      builder.setUserRoleRule(
          UserAttributionTokenRule.newBuilder().setAttributeProjection(attributeProjectionBuilder));
    }
    if (requestHeaderRuleData.hasAuthentication()) {
      builder.setAuthTypeRule(
          getLiteralValueProjectionBuilder(requestHeaderRuleData.getAuthentication().getType()));
    }
  }

  private void convertResponseBodyData(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData
              .ResponseBodyUserAttributionRuleData
          responseBodyRuleData) {
    builder.setTemplate(TEMPLATE_CUSTOM);
    Attribute.Builder responseBodyAttribute =
        Attribute.newBuilder().setResponseBody(PayloadMatch.getDefaultInstance());
    if (responseBodyRuleData.hasCondition()) {
      String urlMatchRegex = responseBodyRuleData.getCondition().getUrlMatchRegex();
      UserAttributionRuleScope.Builder scopeBuilder = builder.getScopeBuilder();
      if (!scopeBuilder.hasUrlScope()) {
        scopeBuilder.setUrlScope(
            UrlScope.newBuilder()
                .setUrlMatchRegexes(StringList.newBuilder().addValues(urlMatchRegex).build()));
      } else {
        UrlScope urlScope = scopeBuilder.getUrlScope();
        if (!urlScope.getUrlMatchRegexes().getValuesList().contains(urlMatchRegex)) {
          urlScope.getUrlMatchRegexes().getValuesList().add(urlMatchRegex);
        }
      }
    }
    if (responseBodyRuleData.hasUserIdLocation()) {
      AttributeProjection.Builder attributeProjectionBuilder = AttributeProjection.newBuilder();
      attributeProjectionBuilder.setAttribute(responseBodyAttribute);
      attributeProjectionBuilder.addAllValueProjections(
          getValueProjections(responseBodyRuleData.getUserIdLocation()));
      builder.setUserIdRule(
          UserAttributionTokenRule.newBuilder().setAttributeProjection(attributeProjectionBuilder));
    }
    if (responseBodyRuleData.hasRoleLocation()) {
      AttributeProjection.Builder attributeProjectionBuilder = AttributeProjection.newBuilder();
      attributeProjectionBuilder.setAttribute(responseBodyAttribute);
      attributeProjectionBuilder.addAllValueProjections(
          getValueProjections(responseBodyRuleData.getRoleLocation()));
      builder.setUserRoleRule(
          UserAttributionTokenRule.newBuilder().setAttributeProjection(attributeProjectionBuilder));
    }
    if (responseBodyRuleData.hasAuthentication()) {
      builder.setAuthTypeRule(
          getLiteralValueProjectionBuilder(responseBodyRuleData.getAuthentication().getType()));
    }
  }

  private void convertCustomJsonData(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData
              .CustomJsonUserAttributionRuleData
          customJsonData) {
    builder.setTemplate(TEMPLATE_CUSTOM);
    if (customJsonData.hasUserIdRuleData()) {
      UserAttributionTokenRule.Builder tokenRuleBuilder = UserAttributionTokenRule.newBuilder();
      tokenRuleBuilder.setCustomProjection(
          CustomProjection.newBuilder().setCustomJson(customJsonData.getUserIdRuleData()));
      builder.setUserIdRule(tokenRuleBuilder);
    }
    if (customJsonData.hasRoleRuleData()) {
      UserAttributionTokenRule.Builder tokenRuleBuilder = UserAttributionTokenRule.newBuilder();
      tokenRuleBuilder.setCustomProjection(
          CustomProjection.newBuilder().setCustomJson(customJsonData.getRoleRuleData()));
      builder.setUserRoleRule(tokenRuleBuilder);
    }
    if (customJsonData.hasAuthTypeRuleData()) {
      UserAttributionTokenRule.Builder tokenRuleBuilder = UserAttributionTokenRule.newBuilder();
      tokenRuleBuilder.setCustomProjection(
          CustomProjection.newBuilder().setCustomJson(customJsonData.getAuthTypeRuleData()));
      builder.setAuthTypeRule(tokenRuleBuilder);
    }
  }

  private void convertCustomToken(
      UserAttributionRuleData.Builder builder,
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomTokenRuleData
          customTokenData)
      throws LegacyUserAttributionRuleTranslationException {
    builder.setTemplate(TEMPLATE_CUSTOM);
    if (customTokenData.hasAuthentication()) {
      UserAttributionTokenRule.Builder tokenRuleBuilder =
          getLiteralValueProjectionBuilder(customTokenData.getAuthentication().getType());
      switch (customTokenData.getLocationCase()) {
        case REQUEST_BODY_LOCATION:
          {
            Predicate.Builder predicateBuilder = Predicate.newBuilder();
            AttributeProjection.Builder attributeProjectionBuilder =
                createAttributeProjectionBuilder(customTokenData.getRequestBodyLocation());
            predicateBuilder.setAttributePredicate(
                Predicate.AttributePredicate.newBuilder()
                    .setAttributeProjection(attributeProjectionBuilder)
                    .setAttributeValueMatchCondition(
                        MatchCondition.newBuilder()
                            .setOperator(ValueMatchOperator.VALUE_MATCH_OPERATOR_NOT_EQUALS)
                            .setMatchValue(
                                LiteralValue.newBuilder()
                                    .setNullValue(LiteralValue.NullValue.getDefaultInstance()))));
            tokenRuleBuilder.setTokenConditionalPredicate(predicateBuilder);
          }
          break;
        case REQUEST_HEADER_LOCATION:
          {
            Predicate.Builder predicateBuilder = Predicate.newBuilder();
            AttributeProjection.Builder attributeProjectionBuilder =
                createAttributeProjectionBuilder(customTokenData.getRequestHeaderLocation());
            predicateBuilder.setAttributePredicate(
                Predicate.AttributePredicate.newBuilder()
                    .setAttributeProjection(attributeProjectionBuilder)
                    .setAttributeValueMatchCondition(
                        MatchCondition.newBuilder()
                            .setOperator(ValueMatchOperator.VALUE_MATCH_OPERATOR_NOT_EQUALS)
                            .setMatchValue(
                                LiteralValue.newBuilder()
                                    .setNullValue(LiteralValue.NullValue.getDefaultInstance()))));
            tokenRuleBuilder.setTokenConditionalPredicate(predicateBuilder);
          }
          break;
        default:
          throw new LegacyUserAttributionRuleTranslationException(
              "Neither request body/header condition exists");
      }
      builder.setAuthTypeRule(tokenRuleBuilder);
    } else {
      throw new LegacyUserAttributionRuleTranslationException("Authentication doesn't exist");
    }
  }

  private AttributeProjection.Builder createAttributeProjectionBuilder(
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation
          requestBodyLocation) {
    AttributeProjection.Builder attributeProjectionBuilder = AttributeProjection.newBuilder();
    attributeProjectionBuilder.setAttribute(
        Attribute.newBuilder().setRequestBody(PayloadMatch.getDefaultInstance()));
    attributeProjectionBuilder.addAllValueProjections(getValueProjections(requestBodyLocation));
    return attributeProjectionBuilder;
  }

  private AttributeProjection.Builder createAttributeProjectionBuilder(
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation
          headerLocation)
      throws LegacyUserAttributionRuleTranslationException {
    AttributeProjection.Builder attributeProjectionBuilder = AttributeProjection.newBuilder();
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        attributeProjectionBuilder.setAttribute(
            Attribute.newBuilder()
                .setRequestHeader(
                    KeyMatch.newBuilder()
                        .setOperator(KEY_MATCH_OPERATOR_EQUALS)
                        .setMatchKey(headerLocation.getHeaderName())));
        break;
      case COOKIE_NAME:
        attributeProjectionBuilder.setAttribute(
            Attribute.newBuilder()
                .setRequestCookie(
                    KeyMatch.newBuilder()
                        .setOperator(KEY_MATCH_OPERATOR_EQUALS)
                        .setMatchKey(headerLocation.getCookieName())));
        break;
      default:
        throw new LegacyUserAttributionRuleTranslationException("Unknown header location case");
    }
    if (headerLocation.hasParsingTarget()) {
      attributeProjectionBuilder.addValueProjections(
          ValueProjection.newBuilder()
              .setRegexCaptureGroup(
                  ValueProjection.RegexCaptureGroupProjection.newBuilder()
                      .setRegex(headerLocation.getParsingTarget().getRegexCaptureGroup()))
              .build());
    }
    return attributeProjectionBuilder;
  }

  private List<ValueProjection> getValueProjections(
      ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation
          encodedLocation) {
    List<ValueProjection> valueProjections = new ArrayList<>();
    if (encodedLocation.hasJsonPath()) {
      valueProjections.add(
          ValueProjection.newBuilder()
              .setJsonPath(
                  ValueProjection.JsonPathProjection.newBuilder()
                      .setPath(encodedLocation.getJsonPath()))
              .build());
    }
    if (encodedLocation.hasParsingTarget()) {
      valueProjections.add(
          ValueProjection.newBuilder()
              .setRegexCaptureGroup(
                  ValueProjection.RegexCaptureGroupProjection.newBuilder()
                      .setRegex(encodedLocation.getParsingTarget().getRegexCaptureGroup()))
              .build());
    }
    return valueProjections;
  }

  private UserAttributionTokenRule.Builder getLiteralValueProjectionBuilder(String value) {
    LiteralValueProjection.Builder literalValueProjectionBuilder =
        LiteralValueProjection.newBuilder();
    literalValueProjectionBuilder.setLiteralValue(LiteralValue.newBuilder().setStringValue(value));
    return UserAttributionTokenRule.newBuilder()
        .setLiteralValueProjection(literalValueProjectionBuilder);
  }

  private static AttributeProjection.Builder createDefaultBasicAuthorizationAttributeBuilder() {
    return AttributeProjection.newBuilder()
        .setAttribute(
            Attribute.newBuilder()
                .setRequestHeader(
                    KeyMatch.newBuilder()
                        .setOperator(KEY_MATCH_OPERATOR_EQUALS)
                        .setMatchKey(DEFAULT_BASIC_AUTHORIZATION_HEADER_NAME)))
        .addValueProjections(
            ValueProjection.newBuilder()
                .setRegexCaptureGroup(
                    ValueProjection.RegexCaptureGroupProjection.newBuilder()
                        .setRegex(DEFAULT_BASIC_AUTHORIZATION_REGEX_CAPTURE_GROUP)))
        .addValueProjections(
            ValueProjection.newBuilder()
                .setBase64(ValueProjection.Base64Projection.getDefaultInstance()))
        .addValueProjections(
            ValueProjection.newBuilder()
                .setRegexCaptureGroup(
                    ValueProjection.RegexCaptureGroupProjection.newBuilder()
                        .setRegex(DEFAULT_BASIC_AUTHORIZATION_USERNAME_REGEX_CAPTURE_GROUP)));
  }
}
