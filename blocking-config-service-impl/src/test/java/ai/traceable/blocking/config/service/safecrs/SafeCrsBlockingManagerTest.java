package ai.traceable.blocking.config.service.safecrs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.blocking.config.service.BlockingConfigServiceConfig;
import ai.traceable.blocking.config.service.UuidGenerator;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SafeCrsBlockingManagerTest {

  private final UuidGenerator uuidGenerator = new UuidGenerator();
  private final String emptyValueUuid = uuidGenerator.generateId("");
  private SafeCrsBlockingManager safeCrsBlockingManager;

  @Test
  void testSafeCrsBlockingRules() {
    // file not present
    assertThrows(
        RuntimeException.class,
        () ->
            new DefaultSafeCrsBlockingManager(
                uuidGenerator,
                new BlockingConfigServiceConfig(
                    ConfigFactory.parseMap(Map.of("safecrs.modsecurity.rules.data.path", "")))));

    safeCrsBlockingManager =
        new DefaultSafeCrsBlockingManager(
            uuidGenerator,
            new BlockingConfigServiceConfig(
                ConfigFactory.parseMap(
                    Map.of("safecrs.modsecurity.rules.data.path", "safecrs/modsecurity.conf"))));
    SafeCrsBlockingRules safeCrsBlockingRules = safeCrsBlockingManager.getBlockingRules("");
    assertNotEquals(emptyValueUuid, safeCrsBlockingRules.getHash());
    assertFalse(safeCrsBlockingRules.getSafeCrsRulesBlob().isEmpty());
  }
}
