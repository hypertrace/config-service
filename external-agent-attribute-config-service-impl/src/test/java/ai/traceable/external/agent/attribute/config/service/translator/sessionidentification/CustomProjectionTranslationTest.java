package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.TestUtils.reader;
import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.CustomProjection;
import com.google.inject.Guice;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CustomProjectionTranslationTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final CustomProjectionTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(CustomProjectionTranslator.class);

  @Test
  void test() throws IOException {

    CustomProjection.Builder builder = CustomProjection.newBuilder();
    PARSER.merge(reader("session_identification/custom_projection/partial_input.json"), builder);
    AttributeRule.Projector projector = translator.translateCustomProjection(builder.build());
    Projector.Builder expectedProjector = Projector.newBuilder();
    PARSER.merge(
        reader("session_identification/custom_projection/partial_output.json"), expectedProjector);
    Assertions.assertEquals(expectedProjector.build(), projector);
  }
}
