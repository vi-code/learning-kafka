import { kafka } from "../../lib/kafka.js";
import { topics } from "../../lib/topics.js";

const admin = kafka.admin();

try {
  await admin.connect();

  const existingTopics = new Set(await admin.listTopics());
  const missingTopics = topics.filter(({ topic }) => !existingTopics.has(topic));

  if (missingTopics.length === 0) {
    console.log("All lab topics already exist.");
  } else {
    await admin.createTopics({
      waitForLeaders: true,
      topics: missingTopics
    });

    console.log("Created topics:");
    for (const { topic, numPartitions } of missingTopics) {
      console.log(`- ${topic} (${numPartitions} partition(s))`);
    }
  }
} finally {
  await admin.disconnect();
}

