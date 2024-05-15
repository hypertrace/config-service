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

class HeaderLocationTranslatorTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final HeaderLocationTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(HeaderLocationTranslator.class);

  @Test
  void testForRequest() throws IOException {

    MatchCondition.Builder builder = MatchCondition.newBuilder();
    PARSER.merge(
        reader("session_identification/location/header/partial_input_for_request.json"), builder);
    List<Projector> projectors =
        translator.translateForRequest(builder.build(), AttributeRule.getDefaultInstance());
    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/location/header/partial_output_for_request.json"),
        eachMatchingProjector);

    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }

  @Test
  void testForContainsKey() throws IOException {

    MatchCondition.Builder builder = MatchCondition.newBuilder();
    PARSER.merge(
        reader("session_identification/location/header/partial_input_for_contains_key.json"),
        builder);
    List<Projector> projectors =
        translator.translateForRequest(builder.build(), AttributeRule.getDefaultInstance());
    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/location/header/partial_output_for_contains_key.json"),
        eachMatchingProjector);

    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }

  @Test
  void testForResponse() throws IOException {
    MatchCondition.Builder builder = MatchCondition.newBuilder();
    PARSER.merge(
        reader("session_identification/location/header/partial_input_for_response.json"), builder);
    List<Projector> projectors =
        translator.translateForResponse(builder.build(), AttributeRule.getDefaultInstance());
    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/location/header/partial_output_for_response.json"),
        eachMatchingProjector);

    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }
}
