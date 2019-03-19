db.meta.insert(
	{
		"inspector" : {
			"dbType" : "http_json",
			"cmds" : [
				"metrics",
			],
			"count" : 60,
			"interval" : 1,
		},
		"key_unique" : "inspector"
	}
);

db.taskList.insert(
	{
		"key_unique" : "inspector",
		"inspector": {
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

