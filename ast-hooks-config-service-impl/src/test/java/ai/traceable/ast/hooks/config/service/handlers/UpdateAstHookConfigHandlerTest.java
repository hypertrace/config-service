package ai.traceable.ast.hooks.config.service.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.ast.hooks.config.service.v1.BasicAuthConfig;
import ai.traceable.ast.hooks.config.service.v1.Bearer;
import ai.traceable.ast.hooks.config.service.v1.DynamicBearer;
import ai.traceable.ast.hooks.config.service.v1.DynamicJwt;
import ai.traceable.ast.hooks.config.service.v1.EncryptedText;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.JwtConfig;
import ai.traceable.ast.hooks.config.service.v1.MutualTls;
import ai.traceable.ast.hooks.config.service.v1.MutualTls.MutualTlsAdvanced;
import ai.traceable.ast.hooks.config.service.v1.Oauth2;
import ai.traceable.ast.hooks.config.service.v1.OauthAuthorizationCodeFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthPasswordFlow;
import ai.traceable.ast.hooks.config.service.v1.PopTokenSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UpdateAstHookConfigHandlerTest {

  private final UpdateAstHookConfigHandler updateAstHookConfigHandler =
      new UpdateAstHookConfigHandler();
  private HookConfig newHookConfig, oldHookConfig;
  private static final String AUTH_ENDPOINT = "auth-endpoint";
  private static final EncryptedText ENCRYPTED_TEXT1 =
      EncryptedText.newBuilder().setKeyId("key-id").setValue("value").build();
  private static final EncryptedText ENCRYPTED_TEXT2 =
      EncryptedText.newBuilder().setKeyId("key-id2").setValue("value2").build();
  private static final EncryptedText ENCRYPTED_TEXT3 =
      EncryptedText.newBuilder().setKeyId("key-id3").setValue("value3").build();
  private static final EncryptedText ENCRYPTED_TEXT_EMPTY = EncryptedText.newBuilder().build();

  @BeforeEach
  void setup() {
    newHookConfig = HookConfig.newBuilder().setAuthEndpoint(AUTH_ENDPOINT).build();
    oldHookConfig = HookConfig.newBuilder().setAuthEndpoint(AUTH_ENDPOINT).build();
  }

  @Test
  void testPopTokenSignature() {
    PopTokenSignature newPopTokenSignature =
        PopTokenSignature.newBuilder()
            .setPopHeaderKey("header-key")
            .setPopPrivateKey(ENCRYPTED_TEXT_EMPTY)
            .build();
    PopTokenSignature oldPopTokenSignature =
        PopTokenSignature.newBuilder().setPopPrivateKey(ENCRYPTED_TEXT1).build();
    newHookConfig = newHookConfig.toBuilder().setPopTokenSignature(newPopTokenSignature).build();
    oldHookConfig = oldHookConfig.toBuilder().setPopTokenSignature(oldPopTokenSignature).build();
    HookConfig result =
        updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(ENCRYPTED_TEXT1, result.getPopTokenSignature().getPopPrivateKey());
  }

  @Test
  void testBearer() {
    Bearer newBearer = Bearer.newBuilder().setBearerToken(ENCRYPTED_TEXT_EMPTY).build();
    Bearer oldBearer =
        Bearer.newBuilder()
            .setDynamicBearer(
                DynamicBearer.newBuilder()
                    .setBasicAuth(
                        BasicAuthConfig.newBuilder()
                            .setPassword(ENCRYPTED_TEXT1)
                            .setUsername("username")))
            .build();
    newHookConfig = newHookConfig.toBuilder().setBearer(newBearer).build();
    oldHookConfig = oldHookConfig.toBuilder().setBearer(oldBearer).build();
    assertThrows(
        RuntimeException.class,
        () -> updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig));

    newBearer =
        Bearer.newBuilder()
            .setDynamicBearer(
                DynamicBearer.newBuilder()
                    .setBasicAuth(BasicAuthConfig.newBuilder().setPassword(ENCRYPTED_TEXT_EMPTY)))
            .build();
    newHookConfig = newHookConfig.toBuilder().setBearer(newBearer).build();
    HookConfig result =
        updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(
        ENCRYPTED_TEXT1, result.getBearer().getDynamicBearer().getBasicAuth().getPassword());
  }

  @Test
  void testOauth() {
    Oauth2 newOauth =
        Oauth2.newBuilder()
            .setToken(ENCRYPTED_TEXT1)
            .setAuthorizationCodeFlow(OauthAuthorizationCodeFlow.newBuilder())
            .build();
    Oauth2 oldOauth =
        Oauth2.newBuilder()
            .setToken(ENCRYPTED_TEXT2)
            .setClientId(ENCRYPTED_TEXT2)
            .setAuthorizationCodeFlow(
                OauthAuthorizationCodeFlow.newBuilder().setClientSecret(ENCRYPTED_TEXT3))
            .build();
    newHookConfig = newHookConfig.toBuilder().setOauth2(newOauth).build();
    oldHookConfig = oldHookConfig.toBuilder().setOauth2(oldOauth).build();
    HookConfig result =
        updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(ENCRYPTED_TEXT1, result.getOauth2().getToken());
    assertEquals(ENCRYPTED_TEXT2, result.getOauth2().getClientId());
    assertEquals(ENCRYPTED_TEXT3, result.getOauth2().getAuthorizationCodeFlow().getClientSecret());

    newOauth =
        Oauth2.newBuilder()
            .setToken(ENCRYPTED_TEXT_EMPTY)
            .setPasswordFlow(OauthPasswordFlow.newBuilder().setClientSecret(ENCRYPTED_TEXT_EMPTY))
            .build();
    newHookConfig = newHookConfig.toBuilder().setOauth2(newOauth).build();
    assertThrows(
        RuntimeException.class,
        () -> updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig));

    oldOauth =
        Oauth2.newBuilder()
            .setToken(ENCRYPTED_TEXT2)
            .setClientId(ENCRYPTED_TEXT2)
            .setPasswordFlow(OauthPasswordFlow.newBuilder().setClientSecret(ENCRYPTED_TEXT3))
            .build();
    oldHookConfig = oldHookConfig.toBuilder().setOauth2(oldOauth).build();
    result = updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(ENCRYPTED_TEXT2, result.getOauth2().getToken());
    assertEquals(ENCRYPTED_TEXT2, result.getOauth2().getClientId());
    assertEquals(ENCRYPTED_TEXT3, result.getOauth2().getPasswordFlow().getClientSecret());
  }

  @Test
  void testMtls() {
    MutualTlsAdvanced mutualTlsAdvanced = MutualTlsAdvanced.newBuilder().build();
    MutualTls newMtls =
        MutualTls.newBuilder()
            .setClientKey(ENCRYPTED_TEXT_EMPTY)
            .setAdvancedOptions(mutualTlsAdvanced)
            .build();
    MutualTls oldMtls =
        MutualTls.newBuilder()
            .setClientKey(ENCRYPTED_TEXT1)
            .setAdvancedOptions(
                MutualTlsAdvanced.newBuilder().setClientKeyPassphrase(ENCRYPTED_TEXT2))
            .build();
    newHookConfig = newHookConfig.toBuilder().setMutualTls(newMtls).build();
    oldHookConfig = oldHookConfig.toBuilder().setMutualTls(oldMtls).build();
    HookConfig result =
        updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(ENCRYPTED_TEXT1, result.getMutualTls().getClientKey());
    assertEquals(
        ENCRYPTED_TEXT2, result.getMutualTls().getAdvancedOptions().getClientKeyPassphrase());

    mutualTlsAdvanced =
        mutualTlsAdvanced.toBuilder().setClientKeyPassphrase(ENCRYPTED_TEXT3).build();
    newMtls = newMtls.toBuilder().setAdvancedOptions(mutualTlsAdvanced).build();
    newHookConfig = newHookConfig.toBuilder().setMutualTls(newMtls).build();
    result = updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(
        ENCRYPTED_TEXT3, result.getMutualTls().getAdvancedOptions().getClientKeyPassphrase());
  }

  @Test
  void testJwtConfig() {
    JwtConfig newJwtConfig = JwtConfig.newBuilder().setJwtToken(ENCRYPTED_TEXT_EMPTY).build();
    JwtConfig oldJwtConfig =
        JwtConfig.newBuilder().setDynamicJwt(DynamicJwt.newBuilder().build()).build();
    newHookConfig = newHookConfig.toBuilder().setJwtConfig(newJwtConfig).build();
    oldHookConfig = oldHookConfig.toBuilder().setJwtConfig(oldJwtConfig).build();
    assertThrows(
        RuntimeException.class,
        () -> updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig));

    newJwtConfig = JwtConfig.newBuilder().setJwtToken(ENCRYPTED_TEXT1).build();
    newHookConfig = newHookConfig.toBuilder().setJwtConfig(newJwtConfig).build();
    HookConfig result =
        updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(ENCRYPTED_TEXT1, result.getJwtConfig().getJwtToken());

    newJwtConfig = JwtConfig.newBuilder().setDynamicJwt(DynamicJwt.newBuilder()).build();
    oldJwtConfig =
        JwtConfig.newBuilder()
            .setDynamicJwt(DynamicJwt.newBuilder().setSecretKey(ENCRYPTED_TEXT2))
            .build();
    newHookConfig = newHookConfig.toBuilder().setJwtConfig(newJwtConfig).build();
    oldHookConfig = oldHookConfig.toBuilder().setJwtConfig(oldJwtConfig).build();
    result = updateAstHookConfigHandler.applyHookConfigUpdate(newHookConfig, oldHookConfig);
    assertEquals(ENCRYPTED_TEXT2, result.getJwtConfig().getDynamicJwt().getSecretKey());
  }
}
