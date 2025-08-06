package ai.traceable.cloud.bot.deployment.config.service.v1.encryption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.bot.deployment.config.service.v1.EncryptedText;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails.JWTSigningAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeyPairGeneratorTest {

  private KeyPairGenerator keyPairGenerator;

  @Mock private CloudBotEncryptionConfig cloudBotEncryptionConfig;

  @BeforeEach
  void setUp() {
    // Mock the encryption behavior
    when(cloudBotEncryptionConfig.encrypt(anyString()))
        .thenAnswer(
            invocation -> {
              String input = invocation.getArgument(0);
              return EncryptedText.newBuilder()
                  .setKeyId("test-key-id")
                  .setValue("encrypted-" + input.substring(0, Math.min(25, input.length())))
                  .build();
            });

    keyPairGenerator = new KeyPairGenerator(cloudBotEncryptionConfig);
  }

  @Test
  void generateJwtSigningKeyPair_shouldGenerateValidKeyPair() {
    JWTSigningKeyDetails keyDetails = keyPairGenerator.generateJwtSigningKeyPair();
    assertEquals(JWTSigningAlgorithm.JWT_SIGNING_ALGORITHM_RS256, keyDetails.getAlgorithm());
    assertTrue(keyDetails.getPublicKeyPem().startsWith("-----BEGIN PUBLIC KEY-----"));

    // Verify encrypted private key
    EncryptedText encryptedPrivateKey = keyDetails.getEncryptedPrivateKey();
    assertNotNull(encryptedPrivateKey);
    assertEquals("test-key-id", encryptedPrivateKey.getKeyId());
    assertTrue(encryptedPrivateKey.getValue().startsWith("encrypted-"));
    assertTrue(
        encryptedPrivateKey.getValue().contains("BEGIN PRIVA"),
        "Encrypted value should contain part of the PEM header");
  }

  @Test
  void generateJwtSigningKeyPair_shouldGenerateUniqueKeys() {
    JWTSigningKeyDetails keyDetails1 = keyPairGenerator.generateJwtSigningKeyPair();
    JWTSigningKeyDetails keyDetails2 = keyPairGenerator.generateJwtSigningKeyPair();
    assertNotEquals(keyDetails1.getPublicKeyPem(), keyDetails2.getPublicKeyPem());
  }
}
