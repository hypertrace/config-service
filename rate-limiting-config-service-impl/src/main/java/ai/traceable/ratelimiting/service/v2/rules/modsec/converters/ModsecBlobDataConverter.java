package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import static ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobConverterUtils.EMPTY_STRING;

import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.ModsecBlobData;
import ai.traceable.ratelimiting.service.v2.rules.modsec.EnrichedRateLimitingModsecRule;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModsecBlobDataConverter {
  private static final long MODSEC_ID_SEED = 20000000;
  private static final String NEW_LINES_DELIMITER = "\n\n";
  private final ModsecBlobConverterUtils modsecBlobConverterUtils;

  @Inject
  public ModsecBlobDataConverter(ModsecBlobConverterUtils modsecBlobConverterUtils) {
    this.modsecBlobConverterUtils = modsecBlobConverterUtils;
  }

  public ModsecBlobData generateModsecBlobData(
      Collection<EnrichedRateLimitingModsecRule> enrichedRateLimitingModsecRules,
      Optional<String> serviceName) {
    AtomicLong modsecIdAssignment = new AtomicLong(MODSEC_ID_SEED);

    List<String> modsecRules = new ArrayList<>();
    enrichedRateLimitingModsecRules.stream()
        .map(
            rule ->
                this.convertRateLimitConditionsToModsecRule(
                    rule.getId(),
                    rule.getUrlRegexes(),
                    rule.getKeyValueConditions(),
                    modsecIdAssignment))
        .filter(Predicate.not(String::isEmpty))
        .forEach(modsecRules::add);

    ModsecBlobData.Builder builder =
        ModsecBlobData.newBuilder()
            .setModsecBlob(String.join(NEW_LINES_DELIMITER, modsecRules))
            .addAllRuleIds(
                enrichedRateLimitingModsecRules.stream()
                    .map(EnrichedRateLimitingModsecRule::getId)
                    .collect(Collectors.toUnmodifiableList()));

    serviceName.ifPresent(builder::addServiceNames);
    return builder.build();
  }

  private String convertRateLimitConditionsToModsecRule(
      String ruleIdentifier,
      Collection<String> urlRegexes,
      Collection<KeyValueCondition> keyValueConditionsList,
      AtomicLong modsecIdAssignment) {
    try {
      return modsecBlobConverterUtils.convertToModsecRule(
          ruleIdentifier,
          modsecBlobConverterUtils.buildUrlRegexClause(urlRegexes),
          keyValueConditionsList.stream()
              .map(modsecBlobConverterUtils::buildKeyValueClause)
              .collect(Collectors.toUnmodifiableList()),
          String.format(
              "URL Regex and key-value conditions corresponding to DLP Rule - %s", ruleIdentifier),
          String.format(
              "Matched all URL Regex and key-value conditions corresponding to DLP Rule - %s",
              ruleIdentifier),
          modsecIdAssignment);
    } catch (Exception e) {
      log.warn("Cannot convert rateLimitingRule with id {} into modsec rule", ruleIdentifier, e);
      return EMPTY_STRING;
    }
  }
}
