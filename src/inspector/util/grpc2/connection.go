// grpc client
package grpc2

import(
	"google.golang.org/grpc"

	"inspector/proto/store"
)

const (
	retryTime = 3
)

// grpc Connection struct
type Connection struct {
	Client store.StoreServiceClient
	Addr   string
	conn   *grpc.ClientConn
}

func NewConnection(addr string) *Connection {
    return &Connection{
	    Addr: addr,
    }
}

func (c *Connection) Close() {
    c.conn.Close()
}

func (c *Connection) EnsureNetwork() bool {
    /*
     * the state may be: Idle/Connecting/Ready/TransientFailure/Shutdown,
     * we only care the "Shutdown"
     */
    if c.conn != nil && c.conn.GetState().String() != "Shutdown" {
        return true
    }

    var err error
    for i := 0; i < retryTime; i++ {
        c.conn, err = grpc.Dial(c.Addr, grpc.WithInsecure())
        if err == nil {
            c.Client = store.NewStoreServiceClient(c.conn)
            return true
        }
    }

    return false
}