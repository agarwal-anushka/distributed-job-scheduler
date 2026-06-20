package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	godotenv.Load()
	// get instance ID from command line argument
	// this lets us run multiple instances with different names
	instanceID := "scheduler-1"
	if len(os.Args) > 1 {
		instanceID = os.Args[1]
	}

	connStr := fmt.Sprintf(
        "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
        os.Getenv("DB_HOST"),
        os.Getenv("DB_PORT"),
        os.Getenv("DB_USER"),
        os.Getenv("DB_PASSWORD"),
        os.Getenv("DB_NAME"),
    )

	db, err := sql.Open("postgres", connStr) //connection to PostgreSQL
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rdb := newRedisClient() //message queue connection

	fmt.Printf("[%s] Starting...\n", instanceID)

	go runForever("worker-short", func() {
		startWorker(rdb, db, "job_queue:short")
	})

	go runForever("worker-long", func() {
		startWorker(rdb, db, "job_queue:long")
	})

	go runForever("worker-default", func() {
		startWorker(rdb, db, "job_queue:default")
	})

	go runForever("watchdog", func() {
		startWatchdog(db, rdb)
	}) // finds and rescue jobs that got stuck

	runWithLeaderElection(instanceID, db, rdb)
	/*This is the main blocking call — it runs the scheduler, but
	only after winning a distributed election. This is what keeps
	multiple instances from all scheduling the same jobs simultaneously. */

}
func runForever(name string, fn func()) {
    for {
        func() {
            defer func() {
                if r := recover(); r != nil {
                    fmt.Printf("%s panicked: %v — restarting in 2s\n", name, r)
                }
            }()
            fn()
        }()
        time.Sleep(2 * time.Second)
    }
}