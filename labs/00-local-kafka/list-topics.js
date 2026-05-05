import { kafka, topicPrefix } from "../../lib/kafka.js";

const admin = kafka.admin();

try {
  await admin.connect();

  const topics = (await admin.listTopics())
    .filter((topic) => topic.startsWith(`${topicPrefix}.`))
    .sort();

  if (topics.length === 0) {
    console.log(`No topics found with prefix "${topicPrefix}."`);
  } else {
    console.log("Lab topics:");
    for (const topic of topics) {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const partitions = metadata.topics[0]?.partitions.length ?? 0;
      console.log(`- ${topic} (${partitions} partition(s))`);
    }
  }
} finally {
  await admin.disconnect();
}

