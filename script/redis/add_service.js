db.meta.insert(
	{
		"redis" : {
			"dbType" : "redis",
			"cmds" : [
				"info",
			],
			"count" : 60,
			"interval" : 1,
			"username" : "",
			"password" : "",
		},
		"key_unique" : "redis"
	}
);

db.taskList.insert(
	{
		"key_unique" : "redis",
		"redis": {
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

