package ai.traceable.blocking.config.service.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SemanticVersioningComparatorTest {
  private static final SemanticVersioningComparator semanticVersioningComparator =
      new SemanticVersioningComparator();

  @Test
  void testComparisons() {
    assertTrue(semanticVersioningComparator.compare("1.2.3-rc.4", "1.2.3-rc.3") > 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3-rc.3", "1.2.3-rc.4") < 0);
    assertEquals(0, semanticVersioningComparator.compare("1.2.3-rc.4", "1.2.3-rc.4"));
    assertTrue(semanticVersioningComparator.compare("2.2.3-rc.4", "1.2.3-rc.4") > 0);
    assertTrue(semanticVersioningComparator.compare("1.3.3-rc.4", "1.2.3-rc.4") > 0);
    assertTrue(semanticVersioningComparator.compare("1.2.4-rc.4", "1.2.3-rc.4") > 0);

    assertTrue(semanticVersioningComparator.compare("1.2.3-dev.4", "1.2.3-dev.3") > 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3-dev.4", "1.2.3-rc.3") < 0);

    assertTrue(semanticVersioningComparator.compare("1.2.3-dev.4", "1.2.3") < 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3-rc.4", "1.2.3") < 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3-rc.4", "1.2.4") < 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3-rc.4", "1.2.2") > 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3-rc.4", "1.1.3") > 0);

    assertTrue(semanticVersioningComparator.compare("1.2.3", "101.2.3") < 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3", "1.12.3") < 0);
    assertTrue(semanticVersioningComparator.compare("1.2.4", "1.2.13") < 0);
    assertTrue(semanticVersioningComparator.compare("1.2.3-rc.4", "1.2.3-rc.13") < 0);
  }
}
