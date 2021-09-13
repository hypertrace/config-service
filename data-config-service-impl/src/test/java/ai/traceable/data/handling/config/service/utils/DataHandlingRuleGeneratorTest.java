package ai.traceable.data.handling.config.service.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleData;
import org.junit.jupiter.api.Test;

class DataHandlingRuleGeneratorTest {

  @Test
  void canTranslateCreateRequestIntoRule() {
    UuidGenerator mockUuidGenerator = mock(UuidGenerator.class);
    when(mockUuidGenerator.generateRandomId()).thenReturn("random-id");

    DataHandlingRuleGenerator generator = new DataHandlingRuleGenerator(mockUuidGenerator);
    DataHandlingRuleData expectedData = DataHandlingRuleData.newBuilder().setName("name").build();

    DataHandlingRule generatedRule =
        generator.generateNewRuleWithoutRank(
            CreateDataHandlingRuleRequest.newBuilder().setData(expectedData).build());

    assertEquals("name", generatedRule.getData().getName());
    assertEquals(expectedData, generatedRule.getData());
    assertEquals("random-id", generatedRule.getId());
  }
}
