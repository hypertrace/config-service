package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.TestUtils.reader;
import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import com.google.inject.Guice;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ValueProjectionsTranslatorTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final ValueProjectionsTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(ValueProjectionsTranslator.class);

  @Test
  void test() throws IOException {

    AttributeProjection.Builder builder = AttributeProjection.newBuilder();
    PARSER.merge(reader("session_identification/value_projections/partial_input.json"), builder);
    AttributeRule attributeRule =
        translator.translateValueProjections(
            builder.getValueProjectionsInOrderList(), AttributeRule.newBuilder());
    AttributeRule expectedAttributeRule =
        TestUtils.getExpectedAttributeRule(
            "session_identification/value_projections/partial_output.json");

    Assertions.assertEquals(expectedAttributeRule, attributeRule);
  }
}
