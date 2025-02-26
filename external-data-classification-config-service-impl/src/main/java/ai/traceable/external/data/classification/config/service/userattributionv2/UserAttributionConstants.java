package ai.traceable.external.data.classification.config.service.userattributionv2;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
class UserAttributionConstants {
  public static final String END_USER_ID_ATTRIBUTE_KEY = "enduser.id";
  public static final String END_USER_ROLE_ATTRIBUTE_KEY = "enduser.role";
  public static final String END_USER_SCOPE_ATTRIBUTE_KEY = "enduser.scope";

  /**
   * these attributes need to be in sync with the ones defined in {@link
   * ai.traceable.external.agent.attribute.config.service.translator.userattributionv2.UserAttributionRuleV2TranslatorImpl}
   */
  public static final String RULE_ATTRIBUTE_KEY_SUFFIX = ".rule";

  public static final String CUSTOM_ATTRIBUTE_PREFIX = "traceableai.custom.attribute.";
}
