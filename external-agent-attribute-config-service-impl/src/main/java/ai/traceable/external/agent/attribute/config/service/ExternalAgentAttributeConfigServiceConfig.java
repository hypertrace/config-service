package ai.traceable.external.agent.attribute.config.service;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.external.agent.attribute.config.service.translator.CustomJsonRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.List;
import lombok.Getter;

@Getter
public class ExternalAgentAttributeConfigServiceConfig {
  private static final String EXTERNAL_AGENT_ATTRIBUTE_CONFIG_SERVICE =
      "external.agent.attribute.config.service";
  private static final String SYSTEM_AUTH_TYPE_RULES = "system.auth.type.rules";
  private static final String SYSTEM_USER_ID_RULES = "system.user.id.rules";
  private static final String SYSTEM_USER_ROLE_RULES = "system.user.role.rules";

  private final Config config;
  private final CustomJsonRuleTranslator customJsonRuleTranslator;

  private final List<AttributeRule> systemAuthTypeRules;
  private final List<AttributeRule> systemUserIdRules;
  private final List<AttributeRule> systemUserRoleRules;

  ExternalAgentAttributeConfigServiceConfig(
      final Config config, final CustomJsonRuleTranslator customJsonRuleTranslator) {
    this.config = config.getConfig(EXTERNAL_AGENT_ATTRIBUTE_CONFIG_SERVICE);
    this.customJsonRuleTranslator = customJsonRuleTranslator;

    this.systemAuthTypeRules = getSystemRules(SYSTEM_AUTH_TYPE_RULES);
    this.systemUserIdRules = getSystemRules(SYSTEM_USER_ID_RULES);
    this.systemUserRoleRules = getSystemRules(SYSTEM_USER_ROLE_RULES);
  }

  private List<AttributeRule> getSystemRules(final String path) {
    return config.hasPath(path)
        ? config.getObjectList(path).stream()
            .map(ConfigObject::render)
            .flatMap(customJsonRuleTranslator::translateRule)
            .collect(toUnmodifiableList())
        : emptyList();
  }
}
