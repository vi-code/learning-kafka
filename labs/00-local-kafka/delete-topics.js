import { kafka, topicPrefix } from "../../lib/kafka.js";

const admin = kafka.admin();

try {
  await admin.connect();

  const topics = (await admin.listTopics()).filter((topic) =>
    topic.startsWith(`${topicPrefix}.`)
  );

  if (topics.length === 0) {
    console.log(`No topics found with prefix "${topicPrefix}."`);
  } else {
    await admin.deleteTopics({ topics });
    console.log("Deleted lab topics:");
    for (const topic of topics.sort()) {
      console.log(`- ${topic}`);
    }
  }
} finally {
  await admin.disconnect();
}

