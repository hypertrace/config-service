package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.AUTH_HEADER_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import java.util.Optional;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class JwtRuleTranslator implements UserAttributionRuleTranslator {

  private static final String BEARER_CAPTURE_GROUP = "^(?:(?i)Bearer:? )?(.*)$";
  private final AttributeKeysExtractor attributeKeysExtractor;
  private final AttributeRuleBuilder attributeRuleBuilder;

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.JWT_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    JwtUserAttributionRuleData data = rule.getData().getJwtData();
    return attributeKeysExtractor
        .getAttributeKeysIfSet(data.getJwtLocation())
        .orElse(AUTH_HEADER_KEYS)
        .stream()
        .map(attributeKey -> translateRuleForUserId(data, attributeKey, rule.getId()))
        .flatMap(Optional::stream);
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    JwtUserAttributionRuleData data = rule.getData().getJwtData();
    return attributeKeysExtractor
        .getAttributeKeysIfSet(data.getJwtLocation())
        .orElse(AUTH_HEADER_KEYS)
        .stream()
        .map(attributeKey -> translateRuleForUserRole(data, attributeKey, rule.getId()))
        .flatMap(Optional::stream);
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    JwtUserAttributionRuleData data = rule.getData().getJwtData();
    return attributeKeysExtractor
        .getAttributeKeysIfSet(data.getJwtLocation())
        .orElse(AUTH_HEADER_KEYS)
        .stream()
        .map(attributeKey -> translateRuleForAuthType(data, attributeKey, rule.getId()))
        .flatMap(Optional::stream);
  }

  private Optional<AttributeRule> translateRuleForUserId(
      JwtUserAttributionRuleData data, String attributeKey, String ruleId) {
    if (data.getUserIdClaim().isEmpty()) {
      return Optional.empty();
    }
    AttributeRule actionRule = attributeRuleBuilder.buildActionAttributeRuleForUserId(ruleId);
    AttributeRule ruleAfterParsingJwtClaim =
        attributeRuleBuilder.buildRuleForParsingTarget(
            data.getUserIdLocation().getParsingTarget(), actionRule);
    if (data.getUserIdLocation().hasJsonPath()) {
      ruleAfterParsingJwtClaim =
          attributeRuleBuilder.buildRuleForJsonPath(
              data.getUserIdLocation().getJsonPath(), ruleAfterParsingJwtClaim);
    }
    AttributeRule ruleForJwtClaim =
        attributeRuleBuilder.buildRuleForJwtClaim(data.getUserIdClaim(), ruleAfterParsingJwtClaim);
    AttributeRule ruleForBearerCaptureGroup =
        attributeRuleBuilder.buildRuleForParsingTarget(
            attributeRuleBuilder.parsingTargetWithFallback(
                data.getJwtLocation().getParsingTarget(), BEARER_CAPTURE_GROUP),
            ruleForJwtClaim);
    boolean isCookieRule =
        data.getJwtLocation().getLocationCase() == HeaderLocation.LocationCase.COOKIE_NAME;
    return Optional.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            isCookieRule
                ? attributeRuleBuilder.buildRuleForCookie(
                    data.getJwtLocation().getCookieName(), ruleForBearerCaptureGroup)
                : ruleForBearerCaptureGroup));
  }

  private Optional<AttributeRule> translateRuleForUserRole(
      JwtUserAttributionRuleData data, String attributeKey, String ruleId) {
    if (data.getRoleClaim().isEmpty()) {
      return Optional.empty();
    }
    AttributeRule actionRule = attributeRuleBuilder.buildActionAttributeRuleForUserRole(ruleId);
    AttributeRule ruleAfterParsingJwtClaim =
        attributeRuleBuilder.buildRuleForParsingTarget(
            data.getRoleLocation().getParsingTarget(), actionRule);
    if (data.getRoleLocation().hasJsonPath()) {
      ruleAfterParsingJwtClaim =
          attributeRuleBuilder.buildRuleForJsonPath(
              data.getRoleLocation().getJsonPath(), ruleAfterParsingJwtClaim);
    }
    AttributeRule ruleForJwtClaim =
        attributeRuleBuilder.buildRuleForJwtClaim(data.getRoleClaim(), ruleAfterParsingJwtClaim);
    AttributeRule ruleForBearerCaptureGroup =
        attributeRuleBuilder.buildRuleForParsingTarget(
            attributeRuleBuilder.parsingTargetWithFallback(
                data.getJwtLocation().getParsingTarget(), BEARER_CAPTURE_GROUP),
            ruleForJwtClaim);
    boolean isCookieRule =
        data.getJwtLocation().getLocationCase() == HeaderLocation.LocationCase.COOKIE_NAME;
    return Optional.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            isCookieRule
                ? attributeRuleBuilder.buildRuleForCookie(
                    data.getJwtLocation().getCookieName(), ruleForBearerCaptureGroup)
                : ruleForBearerCaptureGroup));
  }

  private Optional<AttributeRule> translateRuleForAuthType(
      JwtUserAttributionRuleData data, String attributeKey, String ruleId) {
    if (data.getUserIdClaim().isEmpty() || data.getAuthentication().getType().isEmpty()) {
      return Optional.empty();
    }
    AttributeRule ruleForParsingJwt =
        attributeRuleBuilder.buildRuleForParsingJwt(
            attributeRuleBuilder.buildActionsForAuthType(
                data.getAuthentication().getType(), ruleId));
    AttributeRule ruleForBearerCaptureGroup =
        attributeRuleBuilder.buildRuleForParsingTarget(
            attributeRuleBuilder.parsingTargetWithFallback(
                data.getJwtLocation().getParsingTarget(), BEARER_CAPTURE_GROUP),
            ruleForParsingJwt);
    boolean isCookieRule =
        data.getJwtLocation().getLocationCase() == HeaderLocation.LocationCase.COOKIE_NAME;
    return Optional.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            isCookieRule
                ? attributeRuleBuilder.buildRuleForCookie(
                    data.getJwtLocation().getCookieName(), ruleForBearerCaptureGroup)
                : ruleForBearerCaptureGroup));
  }
}
