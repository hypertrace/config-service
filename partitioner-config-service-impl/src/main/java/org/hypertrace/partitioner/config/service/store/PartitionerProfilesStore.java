package org.hypertrace.partitioner.config.service.store;

import java.util.List;
import java.util.Optional;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;

public interface PartitionerProfilesStore {

  Optional<PartitionerProfile> getPartitionerProfile(String profile) throws Exception;

  List<PartitionerProfile> getAllPartitionProfiles() throws Exception;

  void putPartitionerProfiles(List<PartitionerProfile> partitionerProfiles) throws Exception;

  void deletePartitionerProfiles(List<String> profiles) throws Exception;
}
