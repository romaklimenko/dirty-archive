db = db.getSiblingDB("dirty");

function createIndex(collectionName, data, spec = {}) {
  print(`Creating index for ${collectionName}: ${JSON.stringify(data)}...`);
  db.getCollection(collectionName).createIndex(data, spec);
  print(`Index created for ${collectionName}: ${JSON.stringify(data)} ✅`);
}

createIndex("posts", { id: 1 });
// createIndex("posts", { title: 1 });
createIndex("posts", { created: 1 });
createIndex("posts", { date: 1 });
createIndex("posts", { month: 1 });
createIndex("posts", { year: 1 });
createIndex("posts", { rating: 1 });
// createIndex("posts", { url: 1 });
createIndex("posts", { "user.login": 1 });
createIndex("posts", { "domain.prefix": 1 });
createIndex("posts", { media: 1 });

createIndex("posts", { latest_activity: 1, fetched: 1 });
createIndex("posts", { votes_fetched: 1, id: 1, _id: -1 });
createIndex("posts", { votes_fetched: 1, obsolete: 1, id: 1, _id: -1 });
createIndex("posts", { fetched: 1 });
createIndex("posts", { failed: 1 });
createIndex("posts", { country_code: 1 });
createIndex("posts", { obsolete: 1, failed: 1, fetched: 1 });
createIndex("posts", { id: 1, obsolete: 1, votes_fetched: 1 }, { partialFilterExpression: { obsolete: false } });

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
createIndex("comments", { country_code: 1 });

createIndex("failures", { failed: -1 });
createIndex("failures", { error: 1 });
createIndex("failures", { lock: -1 });

createIndex("media", { usage: 1 });
createIndex("media", { ts: 1 });
createIndex("media", { hash: 1 });
createIndex("media", { filename: 1 });
createIndex("media", { content_type: 1 });
createIndex("media", { length: 1 });
createIndex("media", { error: 1 });
createIndex("media", { selected: 1 });

createIndex("votes", { post_id: 1 });
createIndex("votes", { comment_id: 1 });
createIndex("votes", { from_user_login: 1 });
createIndex("votes", { to_user_login: 1 });
createIndex("votes", { delta: 1 });


print("All indexes are created ✅");
