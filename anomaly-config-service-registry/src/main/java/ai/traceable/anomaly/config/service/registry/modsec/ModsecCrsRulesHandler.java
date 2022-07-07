package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.utils.modsec.ModsecRuleUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import com.google.common.io.Resources;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class ModsecCrsRulesHandler {

  private static final String SEC_RULE = "SecRule";
  private static final String COMMA_DELIMITER = ",";
  private static final String COLON_DELIMITER = ":";
  private static final String NEWLINE_DELIMITER = "\n";
  private static final Pattern ID_PATTERN = Pattern.compile("id:([0-9]+)");
  private static final Pattern MSG_PATTERN = Pattern.compile("msg:'(.*)'");
  private static final Pattern SUB_RULE_TYPE_TAG_PATTERN =
      Pattern.compile("tag:'traceable/type/(.*)'");
  private static final Pattern SUB_RULE_LABELS_TAG_PATTERN =
      Pattern.compile("tag:'traceable/labels/(.*)'");
  private static final String SEC_RULE_REMOVE_BY_ID_FORMAT = "SecRuleRemoveById %d";
  private static final String EMPTY_STRING = "";
  private final ModsecRuleUtils modsecRuleUtils;

  @Inject
  public ModsecCrsRulesHandler(ModsecRuleUtils modsecRuleUtils) {
    this.modsecRuleUtils = modsecRuleUtils;
  }

  /**
   * Method to parse the modsec CRS rules file into separate rules
   *
   * @return map of parent-rule-id to map of sub-rule-id to sub-rule-name E.g. key = crs_913, value
   *     = Map(key = crs_913110, value = {}; key = crs_913120, value = {})
   */
  Map<String, List<AnomalySubRuleInfo>> parseModsecCrsRules(String modsecCrsRulesBlob) {
    Map<String, List<AnomalySubRuleInfo>> modsecRulesMap = new HashMap<>();

    for (String rule : modsecCrsRulesBlob.split(SEC_RULE)) {
      Matcher idMatcher = ID_PATTERN.matcher(rule);
      Matcher msgMatcher = MSG_PATTERN.matcher(rule);
      Matcher tagMatcher = SUB_RULE_TYPE_TAG_PATTERN.matcher(rule);
      Matcher labelMatcher = SUB_RULE_LABELS_TAG_PATTERN.matcher(rule);

      if (idMatcher.find() && msgMatcher.find()) {
        long id = Long.parseLong(idMatcher.group(1));
        String subRuleId = modsecRuleUtils.getModsecRuleId(id);
        String parentId = modsecRuleUtils.getModsecParentRuleId(subRuleId);
        String msg = msgMatcher.group(1).trim();
        if (msg.isEmpty()) {
          throw new RuntimeException(
              String.format(
                  "Modsec rule with id:%s should have a non-empty message to be used as the rule name",
                  id));
        }

        AnomalySubRuleInfo.Builder builder =
            AnomalySubRuleInfo.newBuilder().setRuleId(subRuleId).setRuleName(msg);
        tagMatcher.find();
        Arrays.asList(tagMatcher.group(1).split(COMMA_DELIMITER))
            .forEach(subRuleType -> builder.addSubRuleTypes(getSubRuleType(subRuleType)));
        if (labelMatcher.find()) {
          Arrays.asList(labelMatcher.group(1).split(COMMA_DELIMITER)).stream()
              .map(labelPairString -> labelPairString.split(COLON_DELIMITER))
              .filter(labelPair -> labelPair.length == 2)
              .forEach(labelPair -> builder.putEventLabels(labelPair[0], labelPair[1]));
        }

        if (!modsecRulesMap.containsKey(parentId)) {
          modsecRulesMap.put(parentId, new ArrayList<>());
        }
        modsecRulesMap.get(parentId).add(builder.build());
      }
    }
    return Collections.unmodifiableMap(modsecRulesMap);
  }

  String getModsecCrsBlob(
      String modsecCrsDirectivesFilePath,
      String modsecCrsInitializationRulesFilePath,
      String modsecCrsRulesFilePath,
      Map<String, AnomalyRuleInfo> modsecRules,
      AnomalySubRuleType anomalySubRuleType,
      Set<String> disabledModsecRuleIds) {

    // Ids to be removed should be ordered to ensure the ModSec blob doesn't keep changing on
    // subsequent calls.
    Set<String> idsToBeRemoved = new TreeSet<>();
    disabledModsecRuleIds.forEach(
        ruleId ->
            idsToBeRemoved.add(
                String.format(
                    SEC_RULE_REMOVE_BY_ID_FORMAT,
                    modsecRuleUtils.getModsecCrsRuleIdNumber(ruleId))));

    List<AnomalySubRuleInfo> anomalySubRuleInfoList =
        modsecRules.values().stream()
            .flatMap(ruleInfo -> ruleInfo.getSubRuleInfosList().stream())
            .collect(Collectors.toList());

    anomalySubRuleInfoList.stream()
        .filter(anomalySubRuleInfo -> !matchModsecRule(anomalySubRuleInfo, anomalySubRuleType))
        .forEach(
            anomalySubRuleInfo ->
                idsToBeRemoved.add(
                    String.format(
                        SEC_RULE_REMOVE_BY_ID_FORMAT,
                        modsecRuleUtils.getModsecCrsRuleIdNumber(anomalySubRuleInfo.getRuleId()))));

    if (anomalySubRuleInfoList.size() == idsToBeRemoved.size()) {
      return EMPTY_STRING;
    }

    return String.join(
        NEWLINE_DELIMITER,
        loadModsecCrsFileContents(modsecCrsDirectivesFilePath),
        loadModsecCrsFileContents(modsecCrsInitializationRulesFilePath),
        loadModsecCrsFileContents(modsecCrsRulesFilePath),
        String.join(NEWLINE_DELIMITER, idsToBeRemoved));
  }

  String loadModsecCrsFileContents(String crsFilePath) {
    URL resourceUrl = getClass().getClassLoader().getResource(crsFilePath);
    if (resourceUrl == null) {
      throw new RuntimeException(
          String.format("Unable to locate modsec crs file: %s", crsFilePath));
    } else {
      try {
        return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Unable to read modsec crs file: %s", resourceUrl), e);
      }
    }
  }

  private AnomalySubRuleType getSubRuleType(String modsecCrsSubRuleType) {
    switch (modsecCrsSubRuleType) {
      case "regular":
        return AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR;
      case "safe":
        return AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE;
      case "block":
        return AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK;
      default:
        return AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSPECIFIED;
    }
  }

  private boolean matchModsecRule(
      AnomalySubRuleInfo anomalySubRuleInfo, AnomalySubRuleType anomalySubRuleType) {
    List<AnomalySubRuleType> anomalySubRuleTypeList = anomalySubRuleInfo.getSubRuleTypesList();
    if (anomalySubRuleType == AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSAFE) {
      return anomalySubRuleTypeList.contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
          && !anomalySubRuleTypeList.contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
    } else {
      return anomalySubRuleTypeList.contains(anomalySubRuleType);
    }
  }
}
