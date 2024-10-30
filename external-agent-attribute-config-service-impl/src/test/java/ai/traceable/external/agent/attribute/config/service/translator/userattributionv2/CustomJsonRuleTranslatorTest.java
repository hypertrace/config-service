package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static com.google.inject.Stage.DEVELOPMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v2.CustomProjection;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class CustomJsonRuleTranslatorTest {

  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private final UserAttributionRuleV2Translator userAttributionRuleV2Translator =
      Guice.createInjector(DEVELOPMENT, new UserAttributionRuleV2TranslationModule())
          .getInstance(UserAttributionRuleV2Translator.class);

  @Test
  void testCustomJsonUserAttributionRule() throws IOException {
    UserAttributionRule userAttributionRule =
        UserAttributionRule.newBuilder()
            .setId("some id")
            .setData(
                UserAttributionRuleData.newBuilder()
                    .setUserIdRule(
                        UserAttributionTokenRule.newBuilder()
                            .setCustomProjection(
                                CustomProjection.newBuilder()
                                    .setCustomJson(
                                        Resources.asCharSource(
                                                Resources.getResource(
                                                    "custom_json_v2/user_id_rule_input.json"),
                                                Charset.defaultCharset())
                                            .read()))))
            .build();
    List<AttributeRule> translatedRules =
        userAttributionRuleV2Translator
            .translateRuleForUserId(userAttributionRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, translatedRules.size());
    assertEquals(
        Resources.asCharSource(
                Resources.getResource("custom_json_v2/user_id_rule_output.json"),
                Charset.defaultCharset())
            .read(),
        JSON_PRINTER.print(translatedRules.get(0)));
  }
}
