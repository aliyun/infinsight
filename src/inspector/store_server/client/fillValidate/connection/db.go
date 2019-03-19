package connection

import(
    "time"
    "fmt"
    "errors"

    LOG "github.com/vinllen/log4go"
    "github.com/vinllen/mgo"
    "github.com/vinllen/mgo/bson"
)

type MongoConn struct {
    Session *mgo.Session
    Url string
}

func NewMongoConn(url string, primaryRequired bool) *MongoConn {
    session, err := mgo.Dial(url)
    if err != nil {
        LOG.Critical("Connect to %s failed: %v", url, err)
        return nil
    }

	// maximum pooled connections. the overall established sockets
	// should be lower than this value(will block otherwise)
	session.SetPoolLimit(256)
	session.SetSocketTimeout(10 * time.Minute)

	if err := session.Ping(); err != nil {
		LOG.Critical("Verify ping command to %s failed. %v", url, err)
		return nil
	}

	// Switch the session to a eventually behavior. In that case session
	// may read for any secondary node. default mode is mgo.Strong
	if primaryRequired {
		session.SetMode(mgo.Primary, true)
	} else {
		session.SetMode(mgo.SecondaryPreferred, true)
	}
	LOG.Info("New session to %s successfully", url)
	return &MongoConn{Session: session, Url: url}
}

func (conn *MongoConn) IsGood() bool {
	if err := conn.Session.Ping(); err != nil {
		return false
	}

	return true
}

func (conn *MongoConn) Close() {
	LOG.Info("Close session with %s", conn.Url)
	conn.Session.Close()
}

type DbConnection struct {
    url string
    db string
    collection string
    conn *MongoConn
    oplogsIterator *mgo.Iter
    query bson.M
}

func convertTime(timestamp string) (time.Time, error) {
    t, err := time.ParseInLocation("2006-01-02T15:04:05Z", timestamp, time.Local)
    if err != nil {
        LOG.Error("convert timestamp[%s] error", timestamp)
        return time.Time{}, err
    }
    LOG.Info(t)
    return t, nil
}

func NewDbConnection(url, db, collection, queryStartTime, queryEndTime string) *DbConnection {
    startTime, err := convertTime(queryStartTime)
    if err != nil {
        return nil
    }
    endTime, err := convertTime(queryEndTime)
    if err != nil {
        return nil
    }
    query := bson.M{"t":
        bson.M{
            "$gte": startTime,
            "$lte": endTime,
        },
    }
    // query := bson.M{"h" : "10.101.72.137:9876"}
    return &DbConnection{
        url: url,
        db: db,
        collection: collection,
        query: query,
    }
}

func (c *DbConnection) ensureNetwork() error {
    if c.oplogsIterator != nil {
        return nil
	}

    if c.conn == nil || !c.conn.IsGood() {
        if c.conn != nil {
            c.conn.Close()
        }

        // reconnect
        if c.conn = NewMongoConn(c.url, false); c.conn == nil {
            err := fmt.Errorf("reconnect mongo instance [%s] error", c.url)
			return err
		}
    }

    // c.conn.Session.SetBatch(8192)
	// c.conn.Session.SetPrefetch(0.2)
    c.oplogsIterator = c.conn.Session.DB(c.db).C(c.collection).
		Find(c.query).Iter()
    LOG.Info("query: %v", c.query)
    return nil
}

// Next returns an oplog by raw bytes which is []byte
func (c *DbConnection) Next() (*bson.M, error) {
	if err := c.ensureNetwork(); err != nil {
		return nil, err
	}
	return c.get()
}

// internal get next oplog. used in Next() and NextOplog()
func (c *DbConnection) get() (*bson.M, error) {
	if c.oplogsIterator == nil {
		return nil, errors.New("internal iterator is not valid")
	}

    result := bson.M{}
	if !c.oplogsIterator.Next(result) {
		if err := c.oplogsIterator.Err(); err != nil {
			// some internal error. need rebuild the oplogsIterator
			c.releaseIterator()
			return nil, fmt.Errorf("get next oplog failed. release oplogsIterator, %s", err.Error())
		} else {
			// query timeout
			return nil, fmt.Errorf("timeout!")
		}
	}
	return &result, nil
}

func (c *DbConnection) releaseIterator() {
	if c.oplogsIterator != nil {
		c.oplogsIterator.Close()
	}
	c.oplogsIterator = nil
}
