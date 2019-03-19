package connection

import(
    "time"

    LOG "github.com/vinllen/log4go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"inspector/proto/store"
)

type GrpcConnection struct {
    url string
    conn *grpc.ClientConn
    client store.StoreServiceClient
}

func NewGrpcConnection(url string) *GrpcConnection {
    conn, err := grpc.Dial(url, grpc.WithInsecure())
    if err != nil {
        LOG.Error("Connection to cache error: %s", err.Error())
        return nil
    }

    client := store.NewStoreServiceClient(conn)

    return &GrpcConnection{
        url: url,
        conn: conn,
        client: client,
    }
}

func (g *GrpcConnection) Close() {
    g.conn.Close()
}

func (g *GrpcConnection) ensureNetwork() error {
    if g.conn == nil || g.conn.GetState().String() == "Shutdown" {
        if g.conn == nil {
            g.conn.Close()
        }

        var err error
        g.conn, err = grpc.Dial(g.url, grpc.WithInsecure())
        if err != nil {
            return err
        }
    }
    return nil
}

func (g *GrpcConnection) SendStore(request *store.StoreSaveRequest) (*store.StoreSaveResponse, error) {
    ctx, _ := context.WithTimeout(context.Background(), 5 * time.Second)

    return g.client.Save(ctx, request)
}

func (g *GrpcConnection) SendQuery(request *store.StoreQueryRequest) (*store.StoreQueryResponse, error) {
    ctx, _ := context.WithTimeout(context.Background(), 5 * time.Second)

    return g.client.Query(ctx, request)
}
