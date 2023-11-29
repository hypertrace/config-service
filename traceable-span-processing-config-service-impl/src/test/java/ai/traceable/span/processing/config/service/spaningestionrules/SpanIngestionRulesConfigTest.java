package ai.traceable.span.processing.config.service.spaningestionrules;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRule;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRuleData;
import ai.traceable.span.processing.config.service.v1.Predicate;
import ai.traceable.span.processing.config.service.v1.RetentionAction;
import ai.traceable.span.processing.config.service.v1.StringPredicate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SpanIngestionRulesConfigTest {

  private static final String DEFAULT_RULES_STR =
      "span.processing.config.service {\n"
          + "  default.request.header.retention.rules = [\n"
          + "    {\n"
          + "      \"id\": \"default-rule-id-1\",\n"
          + "      \"data\": {\n"
          + "        \"action\": {\n"
          + "          \"retain\": {}\n"
          + "        },\n"
          + "        \"predicate\": {\n"
          + "          \"target_key_predicate\": {\n"
          + "            \"operator\": \"OPERATOR_EQUALS\",\n"
          + "            \"any_of_values\": {\n"
          + "              \"values\": [\n"
          + "                \"key1\",\n"
          + "                \"key2\"\n"
          + "              ]\n"
          + "            }\n"
          + "          }\n"
          + "        }\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "\n"
          + "  default.response.header.retention.rules = [\n"
          + "    {\n"
          + "      \"id\": \"default-rule-id-2\",\n"
          + "      \"data\": {\n"
          + "        \"action\": {\n"
          + "          \"discard\": {}\n"
          + "        },\n"
          + "        \"predicate\": {\n"
          + "          \"target_key_predicate\": {\n"
          + "            \"operator\": \"OPERATOR_STARTS_WITH\",\n"
          + "            \"value\": \"key3\"\n"
          + "          }\n"
          + "        }\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "\n"
          + "  default.attribute.retention.rules = [\n"
          + "  ]\n"
          + "}";

  @Test
  void testSpanIngestionRulesConfig() {
    Config config = ConfigFactory.parseString(DEFAULT_RULES_STR);
    SpanIngestionRulesConfig spanIngestionRulesConfig = new SpanIngestionRulesConfig(config);

    assertEquals(1, spanIngestionRulesConfig.getDefaultRequestHeaderRetentionRules().size());
    KeyValueRetentionRule expectedDefaultRule1 =
        KeyValueRetentionRule.newBuilder()
            .setId("default-rule-id-1")
            .setData(
                KeyValueRetentionRuleData.newBuilder()
                    .setAction(
                        RetentionAction.newBuilder()
                            .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                    .setPredicate(
                        Predicate.newBuilder()
                            .setTargetKeyPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                    .setAnyOfValues(
                                        StringPredicate.StringList.newBuilder()
                                            .addAllValues(List.of("key1", "key2"))))))
            .build();
    assertEquals(
        expectedDefaultRule1,
        spanIngestionRulesConfig.getDefaultRequestHeaderRetentionRules().get(0));

    assertEquals(1, spanIngestionRulesConfig.getDefaultResponseHeaderRetentionRules().size());
    KeyValueRetentionRule expectedDefaultRule2 =
        KeyValueRetentionRule.newBuilder()
            .setId("default-rule-id-2")
            .setData(
                KeyValueRetentionRuleData.newBuilder()
                    .setAction(
                        RetentionAction.newBuilder()
                            .setDiscard(RetentionAction.DiscardAction.getDefaultInstance()))
                    .setPredicate(
                        Predicate.newBuilder()
                            .setTargetKeyPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(StringPredicate.Operator.OPERATOR_STARTS_WITH)
                                    .setValue("key3"))))
            .build();
    assertEquals(
        expectedDefaultRule2,
        spanIngestionRulesConfig.getDefaultResponseHeaderRetentionRules().get(0));

    assertEquals(0, spanIngestionRulesConfig.getDefaultAttributeRetentionRules().size());

    assertEquals(
        Set.of("default-rule-id-1", "default-rule-id-2"),
        spanIngestionRulesConfig.getDefaultRetentionRuleIds());
  }
}
