package ai.traceable.userattribution.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import org.junit.jupiter.api.Test;

class UserAttributionRuleGeneratorTest {

  private final UserAttributionRuleGenerator generator = new UserAttributionRuleGenerator();

  @Test
  void canTranslateCreateRequestIntoRule() {
    UserAttributionRuleData expectedData =
        UserAttributionRuleData.newBuilder()
            .setRequestHeaderData(
                RequestHeaderUserAttributionRuleData.newBuilder()
                    .setUserIdLocation(HeaderLocation.newBuilder().setHeaderName("user-id-header"))
                    .setRoleLocation(HeaderLocation.newBuilder().setHeaderName("user-role-header")))
            .build();

    UserAttributionRule generatedRule =
        generator.generateNewRuleWithoutRank(
            CreateUserAttributionRuleRequest.newBuilder()
                .setName("name")
                .setData(expectedData)
                .build());

    assertEquals("name", generatedRule.getName());
    assertEquals(expectedData, generatedRule.getData());
    assertFalse(generatedRule.getId().isBlank());
  }
}
