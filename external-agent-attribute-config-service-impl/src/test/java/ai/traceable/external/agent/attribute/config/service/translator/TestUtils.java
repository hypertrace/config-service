package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.FirstMatchingProjector;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import com.google.common.io.Resources;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import lombok.SneakyThrows;

public class TestUtils {

  private static final Parser PARSER = JsonFormat.parser();

  @SneakyThrows
  public static List<AttributeRule> getExpectedAttributeRules(String filename) {
    FirstMatchingProjector.Builder projectorBuilder = FirstMatchingProjector.newBuilder();
    PARSER.merge(reader(filename), projectorBuilder);
    return projectorBuilder.build().getAttributeRulesList();
  }

  @SneakyThrows
  public static AttributeRule getExpectedAttributeRule(String filename) {
    AttributeRule.Builder attributeRuleBuilder = AttributeRule.newBuilder();
    PARSER.merge(reader(filename), attributeRuleBuilder);
    return attributeRuleBuilder.build();
  }

  public static UserAttributionRule getUserAttributionRule(String filename) throws IOException {
    UserAttributionRule.Builder ruleBuilder = UserAttributionRule.newBuilder();
    PARSER.merge(reader(filename), ruleBuilder);
    return ruleBuilder.build();
  }

  @SneakyThrows
  public static SessionIdentificationRule getSessionIdentificationRule(String fileName) {
    SessionIdentificationRule.Builder ruleBuilder = SessionIdentificationRule.newBuilder();
    PARSER.merge(reader(fileName), ruleBuilder);
    return ruleBuilder.build();
  }

  @SneakyThrows
  public static JwtExtractionRule getJwtExtractionRule(String fileName) {
    JwtExtractionRule.Builder ruleBuilder = JwtExtractionRule.newBuilder();
    PARSER.merge(reader(fileName), ruleBuilder);
    return ruleBuilder.build();
  }

  @SneakyThrows
  public static AuthDetectionRule getAuthDetectionRule(String fileName) {
    AuthDetectionRule.Builder ruleBuilder = AuthDetectionRule.newBuilder();
    PARSER.merge(reader(fileName), ruleBuilder);
    return ruleBuilder.build();
  }

  @SneakyThrows
  public static ServiceNamingRule getServiceNamingRule(String fileName) {
    ServiceNamingRule.Builder ruleBuilder = ServiceNamingRule.newBuilder();
    PARSER.merge(reader(fileName), ruleBuilder);
    return ruleBuilder.build();
  }

  @SneakyThrows
  public static Reader reader(String fileName) {
    return Resources.asCharSource(Resources.getResource(fileName), Charset.defaultCharset())
        .openBufferedStream();
  }
}
