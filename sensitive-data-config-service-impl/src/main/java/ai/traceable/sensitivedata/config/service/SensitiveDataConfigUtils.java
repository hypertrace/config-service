package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

public class SensitiveDataConfigUtils {

  public static final String PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG =
      "parameter-type-redaction-strategy-config";
  public static final String AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG =
      "automatic-secret-redaction-strategy-config";
  public static final String REDACTION_RULES_CONFIG = "redaction-rules-config";
  public static final String SENSITIVE_DATA_CONFIGURATION = "sensitive-data-configuration";
  public static final String DEFAULT_RULE_POPULATION_STATUS = "default-rule-population-status";
  public static final String FULL_PRIVACY_MODE_CONFIG = "full-privacy-mode-config";

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
}
