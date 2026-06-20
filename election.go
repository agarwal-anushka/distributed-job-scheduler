package main

import (
    "context"
    "database/sql"
    "fmt"
    "time"
    "os"

    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
    "github.com/redis/go-redis/v9"
)

func newEtcdClient() *clientv3.Client {
        addr := os.Getenv("ETCD_ADDR")
    if addr == "" {
        addr = "localhost:2379"
    }
    client, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"localhost:2379"},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        fmt.Println("Failed to connect to etcd:", err)
        return nil
    }
    return client
}

func runWithLeaderElection(instanceID string, db *sql.DB, rdb *redis.Client) {
    etcdClient := newEtcdClient()
    if etcdClient == nil {
        return
    }
    defer etcdClient.Close()

    for {
        // create a session with 5 second TTL
        // if this process dies, etcd automatically deletes the key after 5 seconds
        session, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(5))
        if err != nil {
            fmt.Println("Failed to create session:", err)
            time.Sleep(2 * time.Second)
            continue
        }

        // create an election under the key "scheduler/leader"
        election := concurrency.NewElection(session, "scheduler/leader")

        fmt.Printf("[%s] Campaigning to become leader...\n", instanceID)

        // Campaign blocks here until this instance wins the election
        // if another instance is already leader, this just waits
        if err := election.Campaign(context.Background(), instanceID); err != nil {
            fmt.Println("Campaign error:", err)
            session.Close()
            continue
        }

        // only reaches here if this instance won
        fmt.Printf("[%s] I am the leader! Starting scheduler...\n", instanceID)
        startScheduler(db, rdb, session)

        // if scheduler ever stops, resign leadership and try again
        election.Resign(context.Background())
        session.Close()
    }
}

func startWatchdog(db *sql.DB, rdb *redis.Client) {
    fmt.Println("Watchdog started — checking for stuck runs every 60 seconds...")
    for {
        time.Sleep(60 * time.Second)

        stuckRuns, err := getStuckRuns(db)
        if err != nil {
            fmt.Println("Watchdog error:", err)
            continue
        }

        if len(stuckRuns) == 0 {
            fmt.Println("Watchdog: no stuck runs found")
            continue
        }

        for _, run := range stuckRuns {
            fmt.Printf("Watchdog: found stuck run_id=%d for job %s — re-queuing\n", run.ID, run.JobName)

            // mark the stuck run as failed
            updateRunStatus(db, run.ID, "failed")

            // re-queue the job
            requeueJob(rdb, run.JobID, run.JobName, run.JobType)
        }
    }
}