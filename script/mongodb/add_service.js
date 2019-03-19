db.meta.insert(
	{
		"mongodb" : {
			"dbType" : "mongodb",
			"cmds" : [
				"serverStatus",
				"replSetGetStatus"
			],
			"count" : 60,
			"interval" : 1,
			"username" : "",
			"password" : "",
		},
		"key_unique" : "mongodb"
	}
);

db.taskList.insert(
	{
		"key_unique" : "mongodb",
		"mongodb": {
			"~key_md5" : 0,
			"distribute" : { }
		}
	}
);

var c = db.taskList.find({"key_unique":"~key_md5"}).count()
if (c == 0) {
	db.taskList.insert(
		{
			"key_unique" : "~key_md5",
			"key_md5" : 0
		}
	);
}

