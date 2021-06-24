package ai.traceable.anomaly.config.service.registry.modsec;

import com.google.common.io.Resources;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;

class ModsecCrsRulesHandler {

  private static final String SEC_RULE = "SecRule";
  private static final Pattern ID_PATTERN = Pattern.compile("id:([0-9]+)");
  private static final Pattern MSG_PATTERN = Pattern.compile("msg:'(.*)'");

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
  Map<String, Map<String, String>> parseModsecCrsRules(String modsecCrsRulesBlob) {

    Map<String, Map<String, String>> modsecRulesMap = new HashMap<>();

    for (String rule : modsecCrsRulesBlob.split(SEC_RULE)) {
      Matcher idMatcher = ID_PATTERN.matcher(rule);
      Matcher msgMatcher = MSG_PATTERN.matcher(rule);

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
        if (!modsecRulesMap.containsKey(parentId)) {
          modsecRulesMap.put(parentId, new HashMap<>());
        }
        modsecRulesMap.get(parentId).put(subRuleId, msg);
      }
    }
    return Collections.unmodifiableMap(modsecRulesMap);
  }

  String getModsecCrsBlob(
      String modsecCrsDirectivesFilePath,
      String modsecCrsInitializationRulesFilePath,
      String modsecCrsRulesFilePath) {
    return String.join(
        "\n",
        loadModsecCrsFileContents(modsecCrsDirectivesFilePath),
        loadModsecCrsFileContents(modsecCrsInitializationRulesFilePath),
        loadModsecCrsFileContents(modsecCrsRulesFilePath));
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
}
