package ai.traceable.cloud.bot.deployment.config.service.v1.encryption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.bot.deployment.config.service.v1.EncryptedText;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import javax.crypto.NoSuchPaddingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CloudBotEncryptionConfigTest {

  @Mock private Config config;
  @Mock private Config encryptionConfig;

  @Test
  void testInitializationWithValidConfig()
      throws NoSuchPaddingException,
          NoSuchAlgorithmException,
          InvalidKeySpecException,
          InvalidKeyException {
    // Generate a real RSA key pair for testing
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(1024);
    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    String publicKeyBase64 = Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded());

    // Setup mock config
    when(config.getConfig("cloudBotDeployment.config.service.encryption"))
        .thenReturn(
            ConfigFactory.parseString(
                "{\n"
                    + "  key_id = \"test-key-id\"\n"
                    + "  public_key = \""
                    + publicKeyBase64
                    + "\"\n"
                    + "  format = \"RSA\"\n"
                    + "  algorithm = \"RSA/ECB/OAEPWithSHA-256AndMGF1Padding\"\n"
                    + "}"));

    CloudBotEncryptionConfig encryptionConfig = new CloudBotEncryptionConfig(config);

    // Test encryption
    String testText = "test-text-to-encrypt";
    EncryptedText encryptedText = encryptionConfig.encrypt(testText);

    assertNotNull(encryptedText);
    assertEquals("test-key-id", encryptedText.getKeyId());
    assertNotEquals(testText, encryptedText.getValue());
  }

  @Test
  void testInitializationWithEmptyKeyId()
      throws NoSuchPaddingException,
          NoSuchAlgorithmException,
          InvalidKeySpecException,
          InvalidKeyException {
    when(config.getConfig("cloudBotDeployment.config.service.encryption"))
        .thenReturn(encryptionConfig);
    when(encryptionConfig.getString("key_id")).thenReturn("");

    CloudBotEncryptionConfig encryptionConfig = new CloudBotEncryptionConfig(config);

    // Test encryption with empty key ID
    String testText = "test-text-to-encrypt";
    EncryptedText encryptedText = encryptionConfig.encrypt(testText);

    assertEquals("", encryptedText.getKeyId());
    assertEquals(testText, encryptedText.getValue()); // Should return the original text
  }
}
