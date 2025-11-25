package ai.traceable.span.processing.config.service.utils;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApiNamingRuleIdGeneratorTest {
  @Test
  void testGenerateGenAiRuleId_sameInputsProduceSameId() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    ApiNamingRuleIdGenerator generator = new ApiNamingRuleIdGenerator(uuidGenerator);

    List<String> regexes = List.of("regex1", "regex2");
    List<String> values = List.of("value1", "value2");

    String id1 = generator.generateGenAiRuleId(regexes, values);
    String id2 = generator.generateGenAiRuleId(regexes, values);

    assertEquals(id1, id2);
  }

  @Test
  void testGenerateGenAiRuleId_differentInputsProduceDifferentIds() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    ApiNamingRuleIdGenerator generator = new ApiNamingRuleIdGenerator(uuidGenerator);

    String id1 = generator.generateGenAiRuleId(List.of("regex1"), List.of("value1"));
    String id2 = generator.generateGenAiRuleId(List.of("regex2"), List.of("value2"));

    assertNotEquals(id1, id2);
  }

  @Test
  void testGenerateGenAiRuleId_orderMatters() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    ApiNamingRuleIdGenerator generator = new ApiNamingRuleIdGenerator(uuidGenerator);

    String id1 = generator.generateGenAiRuleId(List.of("regex1", "regex2"), List.of("value1"));
    String id2 = generator.generateGenAiRuleId(List.of("regex2", "regex1"), List.of("value1"));

    assertNotEquals(id1, id2);
  }
}
