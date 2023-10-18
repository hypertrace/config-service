package ai.traceable.external.data.classification.config.service.session;

import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.common.io.Resources;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SessionIdentificationRulesTranslatorTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final SessionIdentificationRulesTranslator translator =
      new SessionIdentificationRulesTranslator(
          new SessionIdentificationConstants(), new MatchConditionTranslator());

  @Test
  void testForBody() throws IOException {
    SessionIdentificationRule.Builder rule = SessionIdentificationRule.newBuilder();
    PARSER.merge(reader("session_identification/body/complete_input_rule.json"), rule);

    List<DataType> attributeRules =
        translator.translateSessionIdentificationRules(List.of(rule.build()));
    Assertions.assertEquals(2, attributeRules.size());
    DataType.Builder extractedValueBuilder = DataType.newBuilder();
    PARSER.merge(
        reader("session_identification/output_extracted_value_rule_for_response.json"),
        extractedValueBuilder);

    DataType.Builder rawValueBuilder = DataType.newBuilder();
    PARSER.merge(reader("session_identification/body/output_raw_value_rule.json"), rawValueBuilder);
    Assertions.assertEquals(
        List.of(rawValueBuilder.build(), extractedValueBuilder.build()), attributeRules);
  }

  @Test
  void testForHeader() throws IOException {
    SessionIdentificationRule.Builder rule = SessionIdentificationRule.newBuilder();
    PARSER.merge(reader("session_identification/header/complete_input_rule.json"), rule);

    List<DataType> attributeRules =
        translator.translateSessionIdentificationRules(List.of(rule.build()));
    Assertions.assertEquals(2, attributeRules.size());
    DataType.Builder extractedValueBuilder = DataType.newBuilder();
    PARSER.merge(
        reader("session_identification/output_extracted_value_rule_for_response.json"),
        extractedValueBuilder);

    DataType.Builder rawValueBuilder = DataType.newBuilder();
    PARSER.merge(
        reader("session_identification/header/output_raw_value_rule.json"), rawValueBuilder);
    Assertions.assertEquals(
        List.of(rawValueBuilder.build(), extractedValueBuilder.build()), attributeRules);
  }

  @Test
  void testForCookie() throws IOException {
    SessionIdentificationRule.Builder rule = SessionIdentificationRule.newBuilder();
    PARSER.merge(reader("session_identification/cookie/complete_input_rule.json"), rule);

    List<DataType> attributeRules =
        translator.translateSessionIdentificationRules(List.of(rule.build()));
    Assertions.assertEquals(2, attributeRules.size());
    DataType.Builder extractedValueBuilder = DataType.newBuilder();
    PARSER.merge(
        reader("session_identification/output_extracted_value_rule_for_request.json"),
        extractedValueBuilder);

    DataType.Builder rawValueBuilder = DataType.newBuilder();
    PARSER.merge(
        reader("session_identification/cookie/output_raw_value_rule.json"), rawValueBuilder);
    Assertions.assertEquals(
        List.of(rawValueBuilder.build(), extractedValueBuilder.build()), attributeRules);
  }

  @Test
  void testForQueryParam() throws IOException {
    SessionIdentificationRule.Builder rule = SessionIdentificationRule.newBuilder();
    PARSER.merge(reader("session_identification/query_param/complete_input_rule.json"), rule);

    List<DataType> attributeRules =
        translator.translateSessionIdentificationRules(List.of(rule.build()));
    Assertions.assertEquals(2, attributeRules.size());
    DataType.Builder extractedValueBuilder = DataType.newBuilder();
    PARSER.merge(
        reader("session_identification/output_extracted_value_rule_for_request.json"),
        extractedValueBuilder);

    DataType.Builder rawValueBuilder = DataType.newBuilder();
    PARSER.merge(
        reader("session_identification/query_param/output_raw_value_rule.json"), rawValueBuilder);
    Assertions.assertEquals(
        List.of(rawValueBuilder.build(), extractedValueBuilder.build()), attributeRules);
  }

  private static Reader reader(String fileName) throws IOException {
    return Resources.asCharSource(Resources.getResource(fileName), Charset.defaultCharset())
        .openBufferedStream();
  }
}
