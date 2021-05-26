package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.ArrayList;
import java.util.List;

public class SensitiveDataConfigUtils {

  public static final String PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG =
      "parameter-type-redaction-strategy-config";
  public static final String AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG =
      "automatic-secret-redaction-strategy-config";
  public static final String REDACTION_RULES_CONFIG = "redaction-rules-config";
  public static final String SENSITIVE_DATA_CONFIGURATION = "sensitive-data-configuration";

  private SensitiveDataConfigUtils() {
    // to prevent instantiation
  }

  public static PiiFilterConfig toPiiFilterConfig(Config piiFilterConfig) {
    try {
      String jsonString = piiFilterConfig.root().render(ConfigRenderOptions.concise());
      PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
      JsonFormat.parser().merge(jsonString, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<RedactionRule> toRedactionRules(Config redactionRules) {
    try {
      List<ConfigValue> redactionRulesConfigList = redactionRules.getList(REDACTION_RULES_CONFIG);
      List<RedactionRule> redactionRuleList = new ArrayList<>();
      for (ConfigValue redactionRule : redactionRulesConfigList) {
        String jsonString = redactionRule.render(ConfigRenderOptions.concise());
        RedactionRule.Builder builder = RedactionRule.newBuilder();
        JsonFormat.parser().merge(jsonString, builder);
        redactionRuleList.add(builder.build());
      }
      return redactionRuleList;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
