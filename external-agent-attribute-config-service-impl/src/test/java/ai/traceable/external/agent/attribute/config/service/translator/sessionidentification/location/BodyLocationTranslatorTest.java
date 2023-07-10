package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import static ai.traceable.external.agent.attribute.config.service.translator.TestUtils.reader;
import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.SessionIdentificationTranslationModule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.EachMatchingProjector;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import com.google.inject.Guice;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BodyLocationTranslatorTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final BodyLocationTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(BodyLocationTranslator.class);

  @Test
  void testForRequest() throws IOException {

    List<Projector> projectors =
        translator.translateForRequest(
            MatchCondition.getDefaultInstance(), AttributeRule.getDefaultInstance());
    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/location/body/partial_output_for_request.json"),
        eachMatchingProjector);

    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }

  @Test
  void testForResponse() throws IOException {

    List<Projector> projectors =
        translator.translateForResponse(
            MatchCondition.getDefaultInstance(), AttributeRule.getDefaultInstance());
    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/location/body/partial_output_for_response.json"),
        eachMatchingProjector);

    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }
}
