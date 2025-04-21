package ai.traceable.genai.config.service.v1.store;

import static com.google.protobuf.NullValue.NULL_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiScope;
import ai.traceable.genai.config.service.v1.IssuesSummaryFeatureConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GenAiConfigConverterTest {

  private GenAiConfigConverter converter;

  @BeforeEach
  void setUp() {
    converter = new GenAiConfigConverter();
  }

  @Test
  void test_convert_fromGenAiConfig() throws InvalidProtocolBufferException {
    GenAiConfig config =
        GenAiConfig.newBuilder()
            .setScope(GenAiScope.getDefaultInstance())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();

    Value value = converter.convert(config);

    assertNotNull(value);
    assertTrue(value.hasStructValue());
    assertTrue(value.getStructValue().containsFields("scope"));
    assertTrue(value.getStructValue().containsFields("issuesSummaryFeatureConfig"));
  }

  @Test
  void test_convert_fromValueToGenAiConfig() throws InvalidProtocolBufferException {
    GenAiConfig originalConfig =
        GenAiConfig.newBuilder()
            .setScope(GenAiScope.getDefaultInstance())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    Value value = converter.convert(originalConfig);

    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    GenAiConfig convertedConfig = converter.convert(value, builder);

    assertNotNull(convertedConfig);
    assertEquals(originalConfig, convertedConfig);
  }

  @Test
  void test_convert_withNullValue() throws InvalidProtocolBufferException {
    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    GenAiConfig config = converter.convert(null, builder);

    assertNotNull(config);
    assertEquals(GenAiConfig.getDefaultInstance(), config);
  }

  @Test
  void test_convert_withNullValueInValue() throws InvalidProtocolBufferException {
    Value value = Value.newBuilder().setNullValue(NULL_VALUE).build();
    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    GenAiConfig config = converter.convert(value, builder);

    assertNotNull(config);
    assertEquals(GenAiConfig.getDefaultInstance(), config);
  }

  @Test
  void test_convert_withKindNotSet() throws InvalidProtocolBufferException {
    Value value = Value.getDefaultInstance();
    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    GenAiConfig config = converter.convert(value, builder);

    assertNotNull(config);
    assertEquals(GenAiConfig.getDefaultInstance(), config);
  }

  @Test
  void test_convert_withPartialConfig() throws InvalidProtocolBufferException {
    GenAiConfig originalConfig =
        GenAiConfig.newBuilder().setScope(GenAiScope.getDefaultInstance()).build();
    Value value = converter.convert(originalConfig);

    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    GenAiConfig convertedConfig = converter.convert(value, builder);

    assertNotNull(convertedConfig);
    assertTrue(convertedConfig.hasScope());
    assertFalse(convertedConfig.hasIssuesSummaryFeatureConfig());
    assertEquals(originalConfig, convertedConfig);
  }

  @Test
  void test_convert_preservesBuilderState() throws InvalidProtocolBufferException {
    GenAiConfig.Builder builder =
        GenAiConfig.newBuilder()
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build());

    GenAiConfig scopeOnlyConfig =
        GenAiConfig.newBuilder().setScope(GenAiScope.getDefaultInstance()).build();
    Value value = converter.convert(scopeOnlyConfig);

    GenAiConfig result = converter.convert(value, builder);

    assertNotNull(result);
    assertTrue(result.hasScope());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
  }
}
