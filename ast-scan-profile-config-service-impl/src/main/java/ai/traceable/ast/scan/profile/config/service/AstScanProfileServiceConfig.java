package ai.traceable.ast.scan.profile.config.service;

import com.typesafe.config.Config;
import jakarta.inject.Inject;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
public class AstScanProfileServiceConfig {
  private static final String AST_SCAN_PROFILE_SERVICE_CONFIG_NAME =
      "ast.scan.profile.service.config";
  private static final String MAX_USER_INPUT_LENGTH = "maxUserInputLength";
  private int maxUserInputLength;

  @Inject
  public AstScanProfileServiceConfig(Config config) {
    maxUserInputLength =
        config.getConfig(AST_SCAN_PROFILE_SERVICE_CONFIG_NAME).getInt(MAX_USER_INPUT_LENGTH);
  }
}
