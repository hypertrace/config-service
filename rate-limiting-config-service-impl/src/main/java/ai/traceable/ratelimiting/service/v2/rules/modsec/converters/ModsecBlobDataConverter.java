package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import static ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobConverterUtils.EMPTY_STRING;

import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.ModsecBlobData;
import ai.traceable.ratelimiting.service.v2.rules.modsec.EnrichedRateLimitingModsecRule;
import ai.traceable.ratelimiting.service.v2.rules.modsec.validator.ModsecBlobValidator;
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
  private final DataTypeRuleModsecConverter dataTypeRuleModsecConverter;
  private final ModsecBlobValidator modsecBlobValidator;

  @Inject
  public ModsecBlobDataConverter(
      ModsecBlobConverterUtils modsecBlobConverterUtils,
      DataTypeRuleModsecConverter dataTypeRuleModsecConverter,
      ModsecBlobValidator modsecBlobValidator) {
    this.modsecBlobConverterUtils = modsecBlobConverterUtils;
    this.dataTypeRuleModsecConverter = dataTypeRuleModsecConverter;
    this.modsecBlobValidator = modsecBlobValidator;
  }

  public Optional<ModsecBlobData> generateModsecBlobData(
      Collection<EnrichedRateLimitingModsecRule> enrichedRateLimitingModsecRules,
      String tenantId,
      final List<String> serviceNames,
      final List<String> environmentIds) {
    // To protect evaluation of costly data type matching by chaining around them
    List<String> jointUrlRegexes =
        enrichedRateLimitingModsecRules.stream()
            .map(EnrichedRateLimitingModsecRule::getUrlRegexes)
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toUnmodifiableList());

    // Numbering rules to generate unique ids for each modsec rule
    // Using same seed to keep modsec rules blob same if nothing else changes
    AtomicLong modsecIdAssignment = new AtomicLong(MODSEC_ID_SEED);

    List<String> modsecRules = new ArrayList<>();

    // Convert url-regexes and key-value-conditions into modsec blob
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

    // Convert data-type rules into modsec blob
    enrichedRateLimitingModsecRules.stream()
        .map(EnrichedRateLimitingModsecRule::getDataTypeRuleWrappers)
        .flatMap(List::stream)
        .distinct()
        .map(
            dataTypeRuleWrapper ->
                dataTypeRuleModsecConverter.convertToModsecRule(
                    dataTypeRuleWrapper, jointUrlRegexes, environmentIds, modsecIdAssignment))
        .forEach(modsecRules::addAll);

    String modsecBlob = String.join(NEW_LINES_DELIMITER, modsecRules);
    if (!modsecBlobValidator.validate(modsecBlob, tenantId, serviceNames, environmentIds)) {
      return Optional.empty();
    }

    return Optional.of(
        ModsecBlobData.newBuilder()
            .setModsecBlob(modsecBlob)
            .addAllRuleIds(
                enrichedRateLimitingModsecRules.stream()
                    .map(EnrichedRateLimitingModsecRule::getId)
                    .collect(Collectors.toUnmodifiableList()))
            .addAllServiceNames(serviceNames)
            .build());
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
          "Matched URL and request criteria corresponding to DLP Rule",
          modsecIdAssignment);
    } catch (Exception e) {
      log.warn("Cannot convert rateLimitingRule with id {} into modsec rule", ruleIdentifier, e);
      return EMPTY_STRING;
    }
  }
}
