db.mongodb.ensureIndex({"i":1, "t":1});
db.mongodb.createIndex({"e":1}, {expireAfterSeconds:60*60*24})

