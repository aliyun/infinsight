db.redis.ensureIndex({"i":1, "t":1});
db.redis.createIndex({"e":1}, {expireAfterSeconds:60*60*24})

