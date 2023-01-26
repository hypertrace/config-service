package org.hypertrace.partitioner.config.service.store;

import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import java.util.*;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.*;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;

public class PartitionerProfilesDocumentStore implements PartitionerProfilesStore {
  public static final String PARTITIONER_PROFILES = "partitioner_profiles";
  private static final String PARTITIONER_PROFILE_NAME_FIELD = "name";

  private final Collection collection;

  public PartitionerProfilesDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(PARTITIONER_PROFILES);
  }

  @Override
  public Optional<PartitionerProfile> getPartitionerProfile(String profile) throws Exception {
    Query query = new Query();
    query.setFilter(new Filter(Filter.Op.EQ, PARTITIONER_PROFILE_NAME_FIELD, profile));
    try (CloseableIterator<Document> it = collection.search(query)) {
      List<Document> list = ImmutableList.copyOf(it);
      if (list.size() == 1) {
        return Optional.of(PartitionerProfileDocument.fromJson(list.get(0).toJson()));
      } else if (list.size() > 1) {
        throw Status.INTERNAL
            .withDescription("More than 1 PartitionerProfile documents returned")
            .asRuntimeException();
      }
    }
    return Optional.empty();
  }

  @Override
  public List<PartitionerProfile> getAllPartitionProfiles() throws Exception {
    try (CloseableIterator<Document> it = collection.search(new Query())) {
      List<Document> list = ImmutableList.copyOf(it);
      return list.stream()
          .map(document -> PartitionerProfileDocument.fromJson(document.toJson()))
          .collect(Collectors.toList());
    }
  }

  @Override
  public void putPartitionerProfiles(List<PartitionerProfile> partitionerProfiles)
      throws Exception {
    Map<Key, Document> map = new LinkedHashMap<>();
    partitionerProfiles.forEach(
        partitionerProfile ->
            map.put(
                new PartitionerProfileKey(partitionerProfile.getName()),
                new PartitionerProfileDocument(partitionerProfile)));
    this.collection.bulkUpsert(map);
  }

  @Override
  public void deletePartitionerProfiles(List<String> profiles) throws Exception {
    this.collection.delete(
        profiles.stream().map(PartitionerProfileKey::new).collect(Collectors.toSet()));
  }
}
