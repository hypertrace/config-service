package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import com.google.common.io.Resources;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;

class ModsecCrsRulesHandler {

  private static final String SEC_RULE = "SecRule";
  private static final Pattern ID_PATTERN = Pattern.compile("id:([0-9]+),");
  private static final Pattern MSG_PATTERN = Pattern.compile("msg:'(.+)',");

  private final ModsecRuleUtils modsecRuleUtils;

  @Inject
  public ModsecCrsRulesHandler(ModsecRuleUtils modsecRuleUtils) {
    this.modsecRuleUtils = modsecRuleUtils;
  }

  Map<String, List<AnomalySubRuleInfo>> parseModsecCrsRules(String modsecCrsSafeRules) {
    Map<String, List<AnomalySubRuleInfo>> modsecRulesMap = new HashMap<>();

    for (String rule : modsecCrsSafeRules.split(SEC_RULE)) {
      Matcher idMatcher = ID_PATTERN.matcher(rule);
      if (idMatcher.find()) {
        long id = Long.parseLong(idMatcher.group(1));
        String subRuleId = modsecRuleUtils.getModsecRuleId(id);
        String parentId = modsecRuleUtils.getModsecParentRuleId(subRuleId);

        Matcher msgMatcher = MSG_PATTERN.matcher(rule);
        String msg = "";
        if (msgMatcher.find()) {
          msg = msgMatcher.group(1);
        }
        if (msg.isEmpty()) {
          throw new RuntimeException(
              String.format(
                  "Modsec rule with id:%s should have a valid message to be used as the rule name",
                  id));
        }

        if (!modsecRulesMap.containsKey(parentId)) {
          modsecRulesMap.put(parentId, new ArrayList<>());
        }
        modsecRulesMap
            .get(parentId)
            .add(AnomalySubRuleInfo.newBuilder().setRuleId(subRuleId).setRuleName(msg).build());
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
