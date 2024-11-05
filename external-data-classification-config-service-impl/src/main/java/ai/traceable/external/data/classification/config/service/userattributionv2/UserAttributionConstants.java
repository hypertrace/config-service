package ai.traceable.external.data.classification.config.service.userattributionv2;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
class UserAttributionConstants {
  public static final String END_USER_ID_ATTRIBUTE_KEY = "enduser.id";
  public static final String END_USER_ROLE_ATTRIBUTE_KEY = "enduser.role";
  public static final String RULE_ATTRIBUTE_KEY_SUFFIX = ".rule";
}
