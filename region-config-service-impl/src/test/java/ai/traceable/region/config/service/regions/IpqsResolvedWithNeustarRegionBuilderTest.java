package ai.traceable.region.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.region.config.service.RegionConfigServiceConfig;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IpqsResolvedWithNeustarRegionBuilderTest {

  @Test
  void logDropAmount() {
    RegionConfigServiceConfig config =
        new RegionConfigServiceConfig(
            ConfigFactory.parseString(
                "region.config.service {\n"
                    + "\tipqs.countries.data {\n"
                    + "\t\tresolve.neustar = true\n"
                    + "\t\tmode = RESOURCE_FILE\n"
                    + "\t\tresource.file = ipqs/countries.csv\n"
                    + "\t}\n"
                    + "\tneustar.countries.data {\n"
                    + "\t\tmode = RESOURCE_FILE\n"
                    + "\t\tresource.file = neustar/countries.csv\n"
                    + "\t}\n"
                    + "}"));
    IpqsResolvedWithNeustarRegionBuilder ipqsResolvedWithNeustarRegionBuilder =
        new IpqsResolvedWithNeustarRegionBuilder(
            new LatestInstantNamedPathFinder(), new UuidGenerator(), config);
    ipqsResolvedWithNeustarRegionBuilder.getLatestDataSupplier().get();
  }

  @Test
  void testResolution() {
    RegionConfigServiceConfig config =
        new RegionConfigServiceConfig(
            ConfigFactory.parseString(
                "region.config.service {\n"
                    + "\tipqs.countries.data {\n"
                    + "\t\tresolve.neustar = true\n"
                    + "\t\tmode = RESOURCE_FILE\n"
                    + "\t\tresource.file = resolution/ipqs.csv\n"
                    + "\t}\n"
                    + "\tneustar.countries.data {\n"
                    + "\t\tmode = RESOURCE_FILE\n"
                    + "\t\tresource.file = resolution/neustar.csv\n"
                    + "\t}\n"
                    + "}"));
    UuidGenerator uuidGenerator = mock(UuidGenerator.class);
    when(uuidGenerator.generateId((String) any()))
        .thenAnswer(inv -> "uuid-" + inv.getArguments()[0]); // prefix + name
    IpqsResolvedWithNeustarRegionBuilder ipqsResolvedWithNeustarRegionBuilder =
        new IpqsResolvedWithNeustarRegionBuilder(
            new LatestInstantNamedPathFinder(), uuidGenerator, config);
    Map<String, Region> regionMap =
        ipqsResolvedWithNeustarRegionBuilder.getLatestDataSupplier().get();
    assertEquals(
        List.of(new IpV4Range(20, 25), new IpV4Range(35, 45)),
        regionMap.get("uuid-india").getIpV4Ranges());
    assertEquals(List.of(new IpV4Range(56, 93)), regionMap.get("uuid-australia").getIpV4Ranges());
    assertNull(regionMap.get("uuid-bangladesh"));
    assertNull(regionMap.get("uuid-pakistan"));
  }
}
