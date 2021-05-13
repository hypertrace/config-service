package ai.traceable.blocking.config.service.safecrs;

import ai.traceable.blocking.config.service.BlockingConfigServiceConfig;
import ai.traceable.blocking.config.service.UuidGenerator;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

class DefaultSafeCrsBlockingManager implements SafeCrsBlockingManager {

  private final String safeCrsRulesBlob;
  private final String safeCrsRulesHash;

  @Inject
  public DefaultSafeCrsBlockingManager(
      UuidGenerator uuidGenerator, BlockingConfigServiceConfig config) {
    this.safeCrsRulesBlob = loadSafeCrsModsecRules(config);
    safeCrsRulesHash = uuidGenerator.generateId(safeCrsRulesBlob);
  }

  public SafeCrsBlockingRules getBlockingRules(String requestHash) {

    SafeCrsBlockingRules.Builder safeCrsBlockingRulesBuilder =
        SafeCrsBlockingRules.newBuilder().setHash(safeCrsRulesHash);
    if (!safeCrsRulesBlob.isEmpty() && !safeCrsRulesHash.equals(requestHash)) {
      safeCrsBlockingRulesBuilder.setSafeCrsRulesBlob(safeCrsRulesBlob);
    }
    return safeCrsBlockingRulesBuilder.build();
  }

  private String loadSafeCrsModsecRules(BlockingConfigServiceConfig config) {
    String filePath = config.getSafeCrsModsecRulesDataPath();
    if (filePath == null || filePath.isEmpty()) {
      throw new RuntimeException("No file path provided for safe crs modsec rules");
    }
    URL resourceUrl = DefaultSafeCrsBlockingManager.class.getClassLoader().getResource(filePath);
    if (resourceUrl == null) {
      throw new RuntimeException(
          String.format(
              "Unable to locate safe crs modsec rules file: {}",
              config.getSafeCrsModsecRulesDataPath()));
    } else {
      try {
        return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Unable to read safe crs modsec rules file: {}", resourceUrl), e);
      }
    }
  }
}
