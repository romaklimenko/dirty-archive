db = db.getSiblingDB("dirty");

function createIndex(collectionName, data) {
  print(`Creating index for ${collectionName}: ${JSON.stringify(data)}...`);
  db.getCollection(collectionName).createIndex(data);
  print(`Index created for ${collectionName}: ${JSON.stringify(data)} ✅`);
}

createIndex("posts", { id: 1 });
createIndex("posts", { title: 1 });
createIndex("posts", { created: 1 });
createIndex("posts", { date: 1 });
createIndex("posts", { rating: 1 });
createIndex("posts", { url: 1 });
createIndex("posts", { "user.login": 1 });
createIndex("posts", { "domain.prefix": 1 });
createIndex("posts", { media: 1 });
createIndex("posts", { id: 1, fetched: -1 });
createIndex("posts", { fetched: 1 });

createIndex("comments", { id: 1 });
createIndex("comments", { post_id: 1 });
createIndex("comments", { created: 1 });
createIndex("comments", { date: 1 });
createIndex("comments", { rating: 1 });
createIndex("comments", { url: 1 });
createIndex("comments", { "user.login": 1 });
createIndex("comments", { "domain.prefix": 1 });
createIndex("comments", { media: 1 });
createIndex("comments", { body: "text" });
createIndex("comments", { fetched: 1 });

createIndex("failures", { failed: 1 });
createIndex("failures", { error: 1 });

createIndex("media", { usage: 1 });
createIndex("media", { ts: 1 });
createIndex("media", { hash: 1 });
createIndex("media", { filename: 1 });
createIndex("media", { content_type: 1 });
createIndex("media", { length: 1 });
createIndex("media", { error: 1 });
createIndex("media", { selected: 1 });

print("All indexes are created ✅");
