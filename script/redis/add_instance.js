db.taskList.update(
	{ "key_unique" : "redis" },
	{
		$inc : { "~key_md5" : 1 },
		$set : {
			"redis.distribute.127_0_0_1:3002" : {
				"pid" : 0,
				"hid" : 1,
				"host" : "127_0_0_1:3002",
			}
		}
	}
);

db.taskList.update(
	{ "key_unique" : "~key_md5" },
	{
		$inc: { "~key_md5" : 1 }
	}
);

