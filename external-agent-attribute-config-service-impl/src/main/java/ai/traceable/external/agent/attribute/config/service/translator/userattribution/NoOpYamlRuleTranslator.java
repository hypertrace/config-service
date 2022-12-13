package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;

public class NoOpYamlRuleTranslator implements UserAttributionRuleTranslator {

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.CUSTOM_DATA;
  }
}
