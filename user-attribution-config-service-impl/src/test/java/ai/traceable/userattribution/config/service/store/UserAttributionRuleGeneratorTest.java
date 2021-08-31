package ai.traceable.userattribution.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import org.junit.jupiter.api.Test;

class UserAttributionRuleGeneratorTest {

  @Test
  void canTranslateCreateRequestIntoRule() {
    UuidGenerator mockUuidGenerator = mock(UuidGenerator.class);
    when(mockUuidGenerator.generateRandomId()).thenReturn("random-id");
    UserAttributionRuleGenerator generator = new UserAttributionRuleGenerator(mockUuidGenerator);
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
    assertEquals("random-id", generatedRule.getId());
  }
}
