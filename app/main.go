package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"

	"github.com/loov/hrtime"
	"github.com/loov/hrtime/hrplot"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	admin := must(database.NewDatabaseAdminClient(ctx))

	_ = must(admin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/storj/instances/storj",
		DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
		CreateStatement: "CREATE DATABASE alpha",
		ExtraStatements: []string{
			`CREATE TABLE IF NOT EXISTS projects ( project_id BYTES(16) NOT NULL ) PRIMARY KEY ( project_id )`,
		},
	}))
	defer func() {
		must(0, admin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
			Database: "projects/storj/instances/storj/databases/alpha",
		}))
		must(0, admin.Close())
	}()

	f := must(os.Create("cpu.prof"))
	defer f.Close()

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	const N = 10000

	newClient := hrtime.NewStopwatchTSC(N)
	query := hrtime.NewStopwatchTSC(N)
	close := hrtime.NewStopwatchTSC(N)

	for k := 0; k < N; k++ {
		if ctx.Err() != nil {
			break
		}

		start := hrtime.Now()

		lap := newClient.Start()
		client := must(spanner.NewClient(ctx, "projects/storj/instances/storj/databases/alpha"))
		newClient.Stop(lap)

		lap = query.Start()
		stmt := spanner.Statement{SQL: `SELECT count(1) FROM projects`}
		must(0, client.Single().Query(ctx, stmt).Do(
			func(row *spanner.Row) error {
				return nil
			}))
		query.Stop(lap)

		lap = close.Start()
		client.Close()
		close.Stop(lap)

		finish := hrtime.Now()

		if k%100 == 0 {
			fmt.Printf("%v%%  last:%v\n", k*100/N, finish-start)
		}
	}

	hrplot.All("new-client.svg", &BenchmarkInMS{newClient})
	hrplot.All("query.svg", &BenchmarkInMS{query})
	hrplot.All("close.svg", &BenchmarkInMS{close})
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type BenchmarkInMS struct {
	hrplot.Benchmark
}

func (b *BenchmarkInMS) Unit() string {
	return "ms"
}

func (b *BenchmarkInMS) Float64s() []float64 {
	rs := b.Benchmark.Float64s()
	for i := range rs {
		rs[i] *= 1e-6
	}
	return rs
}
