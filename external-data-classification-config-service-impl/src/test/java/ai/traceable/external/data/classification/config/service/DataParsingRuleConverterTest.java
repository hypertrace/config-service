package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.data.parsing.config.service.v1.AttributeFilter;
import ai.traceable.data.parsing.config.service.v1.AttributePredicate;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfig;
import ai.traceable.data.parsing.config.service.v1.DataParsingRule;
import ai.traceable.data.parsing.config.service.v1.SpanFilter;
import ai.traceable.data.parsing.config.service.v1.StringPredicate;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule.DataParsingMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class DataParsingRuleConverterTest {

  @Test
  void testConvertFullRule() {
    // Build internal proto
    DataParsingConfig config =
        DataParsingConfig.newBuilder()
            .setId("id-1")
            .setRank(1)
            .setDataParsingRule(
                DataParsingRule.newBuilder()
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
                    .setAttributeFilter(
                        AttributeFilter.newBuilder().addPrefixes("prefix-1").build())
                    .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_JSON)
                    .setDropOnFailure(DataParsingRule.DropOnFailure.getDefaultInstance())
                    .build())
            .build();

    DataParsingRuleConverter converter = new DataParsingRuleConverter();
    ai.traceable.external.data.classification.config.service.v1.DataParsingRule result =
        converter.convert(List.of(config)).get(0);

    assertEquals(DataParsingMode.DATA_PARSING_MODE_JSON, result.getMode());
    assertEquals(1, result.getSpanFilter().getRequiredMatchingAttributesCount());
    assertEquals(
        "header-x",
        result.getSpanFilter().getRequiredMatchingAttributes(0).getNamePredicate().getValue());
    assertEquals("prefix-1", result.getAttributeFilter().getPrefixes(0));
    // DropOnFailure field should be set (not null)
    assertEquals(true, result.hasDropOnFailure());
  }

  @Test
  void testConvertEmptyRule() {
    DataParsingConfig config =
        DataParsingConfig.newBuilder()
            .setId("id-2")
            .setRank(2)
            .setDataParsingRule(DataParsingRule.newBuilder().build())
            .build();
    DataParsingRuleConverter converter = new DataParsingRuleConverter();
    ai.traceable.external.data.classification.config.service.v1.DataParsingRule result =
        converter.convert(List.of(config)).get(0);
    // Should be default/unspecified
    assertEquals(DataParsingMode.DATA_PARSING_MODE_UNSPECIFIED, result.getMode());
    assertEquals(0, result.getSpanFilter().getRequiredMatchingAttributesCount());
    assertEquals(0, result.getAttributeFilter().getPrefixesCount());
    assertEquals(false, result.hasDropOnFailure());
  }
}
