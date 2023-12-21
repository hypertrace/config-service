package ai.traceable.blocking.config.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.blocking.config.service.v1.BlockingStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ProtoConsistencyTest {

  @Test
  @DisplayName("Blocking status enum consistency between v1 and v2")
  void testBlockingStatusProtoConsistency() {
    assertEquals(
        BlockingStatus.values().length,
        ai.traceable.blocking.config.service.v2.BlockingStatus.values().length);
    for (int i = 0; i < BlockingStatus.values().length - 1; i++) {
      assertEquals(
          BlockingStatus.forNumber(i).name(),
          ai.traceable.blocking.config.service.v2.BlockingStatus.forNumber(i).name());
    }
  }
}
