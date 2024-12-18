package ai.traceable.edge.decision.config.service.aggregator.attributes;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CheckAndAddVariableToRule {
  private static final long DEFAULT_CACHE_EXPIRATION_MINUTES = 10;
  private static final String VARIABLE_REGEX_TEMPLATE =
      "\\b%s\\b"; // Matches the variable as a whole word
  private final Cache<String, Boolean> cache;
  private final UuidGenerator uuidGenerator;

  public CheckAndAddVariableToRule() {
    this.uuidGenerator = new UuidGenerator();
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(DEFAULT_CACHE_EXPIRATION_MINUTES, TimeUnit.MINUTES)
            .build();
  }

  public EdgeDecisionEngineConfig checkAndAddVariableToRule(
      EdgeDecisionEngineConfig edgeDecisionEngineConfig,
      String variableName,
      VariableDerivationMapping variableDerivationMapping) {
    String cacheKey = generateCacheKey(edgeDecisionEngineConfig, variableName);

    // Check cache for existing result
    Boolean isVariablePresent = cache.getIfPresent(cacheKey);
    if (isVariablePresent != null && isVariablePresent) {
      return addVariable(edgeDecisionEngineConfig, variableDerivationMapping);
    }

    // Convert EdgeDecisionRule to string representation
    String ruleRepresentation = edgeDecisionEngineConfig.toString();
    Pattern pattern = Pattern.compile(String.format(VARIABLE_REGEX_TEMPLATE, variableName));
    Matcher matcher = pattern.matcher(ruleRepresentation);

    if (matcher.find()) {
      // Update cache with positive result
      cache.put(cacheKey, true);
      return addVariable(edgeDecisionEngineConfig, variableDerivationMapping);
    }

    // Update cache with negative result
    cache.put(cacheKey, false);
    return edgeDecisionEngineConfig;
  }

  private String generateCacheKey(
      EdgeDecisionEngineConfig edgeDecisionEngineConfig, String variableName) {
    return uuidGenerator.generateId(edgeDecisionEngineConfig) + "#" + variableName;
  }

  private EdgeDecisionEngineConfig addVariable(
      EdgeDecisionEngineConfig edgeDecisionEngineConfig,
      VariableDerivationMapping variableDerivationMapping) {
    // Check if variable is already present
    if (edgeDecisionEngineConfig.getCommonVariablesList().stream()
        .anyMatch(Predicate.isEqual(variableDerivationMapping))) {
      return edgeDecisionEngineConfig;
    } else {
      // Add variable definition
      return edgeDecisionEngineConfig.toBuilder()
          .addCommonVariables(variableDerivationMapping)
          .build();
    }
  }
}
