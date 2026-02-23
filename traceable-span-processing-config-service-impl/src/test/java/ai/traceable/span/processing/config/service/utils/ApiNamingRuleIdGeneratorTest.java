package ai.traceable.span.processing.config.service.utils;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApiNamingRuleIdGeneratorTest {

  @Test
  void testGenerateGenAiRuleId_differentInputsProduceDifferentIds() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    ApiNamingRuleIdGenerator generator = new ApiNamingRuleIdGenerator(uuidGenerator);
    String id1 = generator.generateGenAiRuleId(List.of("regex1"));
    String id2 = generator.generateGenAiRuleId(List.of("regex2"));

    assertNotEquals(id1, id2);
  }

  @Test
  void testGenerateGenAiRuleId_orderMatters() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    ApiNamingRuleIdGenerator generator = new ApiNamingRuleIdGenerator(uuidGenerator);
    String id1 = generator.generateGenAiRuleId(List.of("regex1", "regex2"));
    String id2 = generator.generateGenAiRuleId(List.of("regex2", "regex1"));
    assertNotEquals(id1, id2);
  }
}
