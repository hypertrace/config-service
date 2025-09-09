package ai.traceable.data.parsing.config.service.v1.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.data.parsing.config.service.v1.AttributeFilter;
import ai.traceable.data.parsing.config.service.v1.AttributePredicate;
import ai.traceable.data.parsing.config.service.v1.DataParsingRule;
import ai.traceable.data.parsing.config.service.v1.SpanFilter;
import ai.traceable.data.parsing.config.service.v1.StringPredicate;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataParsingConfigRequestValidatorTest {
  private DataParsingConfigRequestValidator validator;

  @BeforeEach
  void setup() {
    validator = new DataParsingConfigRequestValidator();
  }

  @Test
  void testValidRule_minimal() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(rule));
  }

  @Test
  void testValidRule_withSpanAndAttributeFilter() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_XML)
            .setSpanFilter(
                SpanFilter.newBuilder()
                    .addRequiredMatchingAttributes(
                        AttributePredicate.newBuilder()
                            .setNamePredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(
                                        ai.traceable.data.parsing.config.service.v1.Operator
                                            .OPERATOR_EQUALS)
                                    .setValue("header-x")
                                    .build())
                            .build())
                    .build())
            .setAttributeFilter(AttributeFilter.newBuilder().addPrefixes("prefix-1").build())
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(rule));
  }

  @Test
  void testInvalid_missingMode() {
    DataParsingRule rule = DataParsingRule.newBuilder().build();
    assertThrows(StatusRuntimeException.class, () -> validator.validateOrThrow(rule));
  }

  @Test
  void testInvalid_emptyAttributeFilter() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .setAttributeFilter(AttributeFilter.newBuilder().build())
            .build();
    assertThrows(StatusRuntimeException.class, () -> validator.validateOrThrow(rule));
  }

  @Test
  void testInvalid_emptySpanFilter() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .setSpanFilter(SpanFilter.newBuilder().build())
            .build();
    assertThrows(StatusRuntimeException.class, () -> validator.validateOrThrow(rule));
  }

  @Test
  void testInvalid_requiredMatchingAttribute_missingNamePredicate() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .setSpanFilter(
                SpanFilter.newBuilder()
                    .addRequiredMatchingAttributes(AttributePredicate.newBuilder().build())
                    .build())
            .build();
    assertThrows(StatusRuntimeException.class, () -> validator.validateOrThrow(rule));
  }

  @Test
  void testInvalid_requiredMatchingAttribute_invalidStringPredicate() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .setSpanFilter(
                SpanFilter.newBuilder()
                    .addRequiredMatchingAttributes(
                        AttributePredicate.newBuilder()
                            .setNamePredicate(
                                StringPredicate.newBuilder().build()) // missing value/operator
                            .build())
                    .build())
            .build();
    assertThrows(StatusRuntimeException.class, () -> validator.validateOrThrow(rule));
  }

  @Test
  void testValid_requiredMatchingAttribute_withValuePredicate() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .setSpanFilter(
                SpanFilter.newBuilder()
                    .addRequiredMatchingAttributes(
                        AttributePredicate.newBuilder()
                            .setNamePredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(
                                        ai.traceable.data.parsing.config.service.v1.Operator
                                            .OPERATOR_EQUALS)
                                    .setValue("header-x")
                                    .build())
                            .setValuePredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(
                                        ai.traceable.data.parsing.config.service.v1.Operator
                                            .OPERATOR_MATCHES_REGEX)
                                    .setValue("val-regex")
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(rule));
  }

  @Test
  void testInvalid_environmentScope_emptyIds() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .setEnvironmentScope(DataParsingRule.EnvironmentScope.newBuilder().build())
            .build();
    assertThrows(StatusRuntimeException.class, () -> validator.validateOrThrow(rule));
  }

  @Test
  void testValid_environmentScope_withIds() {
    DataParsingRule rule =
        DataParsingRule.newBuilder()
            .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
            .setEnvironmentScope(
                DataParsingRule.EnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(rule));
  }
}
