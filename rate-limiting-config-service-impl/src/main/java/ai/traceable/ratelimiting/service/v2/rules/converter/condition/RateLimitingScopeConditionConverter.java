package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.SCOPE_CONDITION;

import ai.traceable.datamodel.data.transformation.config.v1.JexlScriptConfig;
import ai.traceable.edge.decision.config.service.v1.GenericMatchCondition;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;

public class RateLimitingScopeConditionConverter implements RateLimitingConditionConverter {

  @Override
  public MatchCondition buildMatchCondition(final LeafCondition leafCondition) {
    final ScopeCondition scopeCondition = leafCondition.getScopeCondition();
    final GenericMatchCondition.Builder builder = GenericMatchCondition.newBuilder();
    String regexes = null;
    switch (scopeCondition.getScopeCase()) {
      case URL_SCOPE:
        regexes = String.join("|", scopeCondition.getUrlScope().getUrlRegexesList());
        break;
      case ENTITY_SCOPE:
      case LABEL_SCOPE:
      default:
        throw new IllegalArgumentException("Unknown scope case: " + scopeCondition.getScopeCase());
    }
    builder.setJexlScript(
        JexlScriptConfig.newBuilder()
            .setJexlScript("var regex = " + regexes + "; regex.test($s.getRequestUrl())"));
    return MatchCondition.newBuilder().setGenericMatchCondition(builder).build();
  }

  @Override
  public ConditionCase getConditionCase() {
    return SCOPE_CONDITION;
  }
}
