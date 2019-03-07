/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
create table example.tweet(timeline text, id UUID, text int, PRIMARY KEY(id));
create index on example.tweet(timeline);
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gocql/gocql"
)

type hostMetrics struct {
	attempts int
	latency  int
}

// The observer type to watch the queries data
type testQueryObserver struct {
	metrics map[string]*hostMetrics
	verbose bool
}

func (o *testQueryObserver) ObserveQuery(ctx context.Context, q gocql.ObservedQuery) {
	host := q.Host.ConnectAddress().String()
	curMetric := o.metrics[host]
	curAttempts := 0
	curLatency := 0
	if curMetric != nil {
		curAttempts = curMetric.attempts
		curLatency = curMetric.latency
	}
	if q.Err == nil {
		o.metrics[host] = &hostMetrics{attempts: q.Metrics.Attempts + curAttempts, latency: curLatency + int(q.Metrics.TotalLatency/1000000)}
	}
	if o.verbose {
		fmt.Printf("Observed query %q. Returned %v rows, took %v on host %q with %v attempts and total latency %v. Error: %q\n",
			q.Statement, q.Rows, q.End.Sub(q.Start), host, q.Metrics.Attempts, q.Metrics.TotalLatency, q.Err)
	}
}

func (o *testQueryObserver) GetMetrics() {
	for h, m := range o.metrics {
		fmt.Printf("Host: %s, Attempts: %v, Avg Latency: %vms\n", h, m.attempts, m.latency/m.attempts)
	}
}

// Simple retry policy for attempting the connection to 1 host only per query
type RT struct {
	num int
}

func (rt *RT) Attempt(q gocql.RetryableQuery) bool {
	return q.Attempts() <= rt.num
}

func (rt *RT) GetRetryType(err error) gocql.RetryType {
	return gocql.Rethrow
}

func main() {

	specExec := flag.Bool("specExec", false, "Speculative execution")
	flag.Parse()

	// the number of entries to insert
	cycles := 10000

	// connect to the cluster
	cluster := gocql.NewCluster("...")
	cluster.Keyspace = "example"

	// the timeout of one of the nodes is very high, so let’s make sure we wait long enough
	cluster.Timeout = 10 * time.Second
	cluster.RetryPolicy = &RT{num: 3}
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	observer := &testQueryObserver{metrics: make(map[string]*hostMetrics), verbose: false}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < cycles; i = i + 1 {
		r := rand.Intn(10000)
		u, _ := gocql.RandomUUID()
		query := session.Query(`INSERT INTO example.tweet (id, timeline, data) VALUES (?, 'me', ?)`, u, r).Observer(observer)
		// Create speculative execution policy with the timeout delay between following executions set to 10ms
		sp := &gocql.SimpleSpeculativeExecution{NumAttempts: 2, TimeoutDelay: 10 * time.Millisecond}
		// Specifically set Idempotence to either true or false to constrol normal/speculative execution
		query.SetSpeculativeExecutionPolicy(sp).Idempotent(*specExec)
		query.Exec()
	}

	// wait a sec before everything finishes
	<-time.After(1 * time.Second)

	// Print results
	fmt.Println("\n==========\n")
	observer.GetMetrics()
}
