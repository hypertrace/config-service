package ai.traceable.data.parsing.config.service.v1.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.parsing.config.service.v1.AttributeFilter;
import ai.traceable.data.parsing.config.service.v1.CreateDataParsingRuleRequest;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfig;
import ai.traceable.data.parsing.config.service.v1.DataParsingRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataParsingConfigGeneratorTest {

  @Mock private UuidGenerator uuidGenerator;
  private DataParsingConfigGenerator generator;

  @BeforeEach
  void setup() {
    generator = new DataParsingConfigGenerator(uuidGenerator);
  }

  @Test
  void generateNewConfig_shouldSetIdAndDataParsingRule() {
    when(uuidGenerator.generateRandomId()).thenReturn("test-uuid");

    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setEnabled(true)
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_URL_ENCODED)
            .setAttributeFilter(AttributeFilter.newBuilder().addPrefixes("http.").build())
            .build();

    CreateDataParsingRuleRequest request =
        CreateDataParsingRuleRequest.newBuilder().setDataParsingRule(rule).build();

    DataParsingConfig config = generator.generateNewConfig(request);

    assertNotNull(config);
    assertEquals("test-uuid", config.getId());
    assertEquals(rule, config.getDataParsingRule());
  }
}
