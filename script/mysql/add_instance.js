db.taskList.update(
	{ "key_unique" : "mysql" },
	{
		$inc : { "~key_md5" : 1 },
		$set : {
			"mysql.distribute.127_0_0_1:3306" : {
				"pid" : 0,
				"hid" : 1,
				"host" : "127_0_0_1:3306",
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

