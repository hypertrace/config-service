package ai.traceable.cloud.bot.deployment.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class CloudBotDeploymentConfigServiceFactoryTest {

  @Test
  void testModuleBindings() throws NoSuchAlgorithmException {
    Channel channel = mock(Channel.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    Config config = mock(Config.class);

    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(1024);
    KeyPair kp = kpg.generateKeyPair();
    String publicKeyString = Base64.getEncoder().encodeToString(kp.getPublic().getEncoded());

    when(config.getConfig("cloudBotDeployment.config.service.encryption"))
        .thenReturn(
            ConfigFactory.parseString(
                "{\n"
                    + "  key_id = \"\"\n"
                    + "  public_key = \""
                    + publicKeyString
                    + "\"\n"
                    + "  format = \"RSA\"\n"
                    + "  algorithm = \"RSA/ECB/OAEPWithSHA-256AndMGF1Padding\"\n"
                    + "}"));

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new CloudBotDeploymentConfigServiceModule(
                        channel, config, configChangeEventGenerator))
                .getAllBindings());
  }
}
