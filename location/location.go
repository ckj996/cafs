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
	source   map[string]int64
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
	return Loc{client: c, conn: conn, hostname: hn, source: make(map[string]int64)}
}

func (loc *Loc) Close() {
	loc.conn.Close()
}

func (loc *Loc) Query(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := loc.client.Query(ctx, &pb.QueryRequest{Key: key})
	if err != nil || r.GetLocation() == "" {
		return "", err
	}
	url := "http://" + r.GetLocation() + "/" + key
	loc.source[key] = r.GetSource()
	return url, nil
}

func (loc *Loc) Report(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := loc.client.Report(ctx, &pb.ReportRequest{Key: key, Location: loc.hostname, Source: loc.source[key]})
	if err == nil {
		delete(loc.source, key)
	}
	return err
}
