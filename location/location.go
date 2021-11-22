package location

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/kaijchen/tracker/track"
	"google.golang.org/grpc"
)

type Loc struct {
	client   pb.TrackerClient
	conn     *grpc.ClientConn
	hostname string
}

func NewLoc(addr string) Loc {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	c := pb.NewTrackerClient(conn)
	hn, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get hostname: %v", err)
	}
	return Loc{client: c, conn: conn, hostname: hn}
}

func (loc *Loc) Close() {
	loc.conn.Close()
}

func (loc *Loc) Query(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := loc.client.Query(ctx, &pb.QueryRequest{Key: key})
	if err != nil {
		return "", err
	}
	url := "http://" + r.GetLocation() + "/" + key
	return url, nil
}

func (loc *Loc) Report(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := loc.client.Report(ctx, &pb.ReportRequest{Key: key, Location: loc.hostname})
	return err
}
