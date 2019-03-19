db.taskList.update(
	{ "key_unique" : "inspector" },
	{
		$inc : { "~key_md5" : 1 },
		$set : {
			"inspector.distribute.127_0_0_1:7300" : {
				"pid" : 0,
				"hid" : 1,
				"host" : "127_0_0_1:7300",
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

