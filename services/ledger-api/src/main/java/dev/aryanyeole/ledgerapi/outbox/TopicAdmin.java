package dev.aryanyeole.ledgerapi.outbox;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

// The admin calls the topic guards share (D-080, D-110): a short-lived Admin client with bounded
// timeouts, a topic's partition count (-1 if absent), creating a topic (a concurrent creation by
// another relay counts as created; the caller describes it like any existing topic), and one
// config value read with describeConfigs. Nothing here alters an existing topic.
final class TopicAdmin {

	private TopicAdmin() {
	}

	static Admin open(List<String> bootstrapServers, int timeoutMillis, String clientId) {
		return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapServers),
				AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMillis,
				AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMillis, AdminClientConfig.CLIENT_ID_CONFIG,
				clientId));
	}

	// Partition count of the topic, or -1 if it doesn't exist.
	static int partitionCount(Admin admin, String topic, int timeoutMillis)
			throws InterruptedException, ExecutionException, TimeoutException {
		try {
			TopicDescription description = admin.describeTopics(Set.of(topic))
				.allTopicNames()
				.get(timeoutMillis, TimeUnit.MILLISECONDS)
				.get(topic);
			return description.partitions().size();
		}
		catch (ExecutionException ex) {
			if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
				return -1;
			}
			throw ex;
		}
	}

	// Creates the topic; true if this call created it, false if it already existed (created
	// concurrently, e.g. by another relay instance).
	static boolean create(Admin admin, NewTopic topic, int timeoutMillis)
			throws InterruptedException, ExecutionException, TimeoutException {
		try {
			admin.createTopics(Set.of(topic)).all().get(timeoutMillis, TimeUnit.MILLISECONDS);
			return true;
		}
		catch (ExecutionException ex) {
			if (ex.getCause() instanceof TopicExistsException) {
				return false;
			}
			throw ex;
		}
	}

	// A topic config value as the broker reports it (the topic override, or the broker default).
	static String config(Admin admin, String topic, String key, int timeoutMillis)
			throws InterruptedException, ExecutionException, TimeoutException {
		ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
		Config config = admin.describeConfigs(Set.of(resource))
			.all()
			.get(timeoutMillis, TimeUnit.MILLISECONDS)
			.get(resource);
		ConfigEntry entry = (config != null) ? config.get(key) : null;
		return (entry != null) ? entry.value() : null;
	}

}
