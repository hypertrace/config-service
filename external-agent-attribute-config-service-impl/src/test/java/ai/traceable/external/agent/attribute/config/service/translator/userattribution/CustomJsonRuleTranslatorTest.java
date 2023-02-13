package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import com.google.common.io.Resources;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CustomJsonRuleTranslatorTest {

  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private final CustomJsonRuleTranslator customJsonRuleTranslator = new CustomJsonRuleTranslator();

  @Test
  void testCustomJsonUserAttributionRule() throws IOException {
    UserAttributionRule userAttributionRule =
        UserAttributionRule.newBuilder()
            .setId("some id")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setCustomJsonData(
                        UserAttributionRuleData.CustomJsonUserAttributionRuleData.newBuilder()
                            .setUserIdRuleData(
                                Resources.asCharSource(
                                        Resources.getResource(
                                            "custom_json/user_id_rule_input.json"),
                                        Charset.defaultCharset())
                                    .read())
                            .build())
                    .build())
            .build();
    List<AttributeRule> translatedRules =
        customJsonRuleTranslator
            .translateRuleForUserId(userAttributionRule)
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(1, translatedRules.size());
    Assertions.assertEquals(
        Resources.asCharSource(
                Resources.getResource("custom_json/user_id_rule_output.json"),
                Charset.defaultCharset())
            .read(),
        JSON_PRINTER.print(translatedRules.get(0)));
  }
}
