package ai.traceable.threatscoring.config.service;

import static ai.traceable.threatscoring.config.service.store.ScopedThreatScoringConfigsStore.DEFAULT_THREAT_SCORING_CONTEXT;

import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigScope;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ThreatScoringConfigScopeUtils {

  List<String> getContextsWithIncreasingPriority(ThreatScoringConfigScope configScope) {
    /*
     * Precedence Order -->  environment > customerConfig > defaultConfig For example, if
     */
    List<String> contextsWithIncreasingPriority = new ArrayList<>();
    contextsWithIncreasingPriority.add(DEFAULT_THREAT_SCORING_CONTEXT);

    switch (configScope.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        contextsWithIncreasingPriority.add(configScope.getEnvironmentScope().getEnvironmentId());
        break;
      case SCOPE_NOT_SET:
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }
    return contextsWithIncreasingPriority;
  }
}
