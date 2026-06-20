// load_test.go
//
// Standalone load test for the Distributed Job Scheduler.
// Run this in a SEPARATE terminal while your scheduler (main.go) is already running.
//
// What it does:
//   1. Inserts N test jobs, all due immediately, split evenly across job types
//   2. Polls the database every second to see how many have completed
//   3. Once all jobs have a finished run, prints total time and throughput
//
// Usage:
//   go run load_test.go 200
//   (200 = number of test jobs to create; defaults to 100 if omitted)

package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	godotenv.Load()

	numJobs := 100
	if len(os.Args) > 1 {
		if n, err := strconv.Atoi(os.Args[1]); err == nil {
			numJobs = n
		}
	}

	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME"),
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Println("Failed to connect to database:", err)
		os.Exit(1)
	}
	defer db.Close()

	runTag := fmt.Sprintf("loadtest_%d", time.Now().Unix())
	jobTypes := []string{"short", "long", "default"}

	fmt.Printf("Inserting %d test jobs (tag: %s)...\n", numJobs, runTag)

	for i := 0; i < numJobs; i++ {
		jobName := fmt.Sprintf("%s_job_%d", runTag, i)
		jobType := jobTypes[i%len(jobTypes)]

		_, err := db.Exec(
			`INSERT INTO jobs (name, cron_expr, status, next_run_at, job_type)
			 VALUES ($1, '* * * * *', 'active', NOW(), $2)`,
			jobName, jobType,
		)
		if err != nil {
			fmt.Println("Failed to insert job:", err)
			os.Exit(1)
		}
	}

	fmt.Println("Jobs inserted. Waiting for scheduler to pick them up...")
	fmt.Println("(make sure main.go is already running in another terminal)")
	fmt.Println()

	startTime := time.Now()

	for {
		var completedCount int
		err := db.QueryRow(`
			SELECT COUNT(DISTINCT j.id)
			FROM jobs j
			JOIN job_runs jr ON jr.job_id = j.id
			WHERE j.name LIKE $1
			AND jr.status = 'success'
		`, runTag+"%").Scan(&completedCount)

		if err != nil {
			fmt.Println("Error checking progress:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		elapsed := time.Since(startTime)
		fmt.Printf("\r%d / %d jobs completed (%.0fs elapsed)   ", completedCount, numJobs, elapsed.Seconds())

		if completedCount >= numJobs {
			fmt.Println()
			fmt.Println()
			printResults(numJobs, elapsed)
			break
		}

		// safety timeout — stop after 5 minutes so the test doesn't hang forever
		if elapsed > 5*time.Minute {
			fmt.Println()
			fmt.Println()
			fmt.Printf("Timed out after 5 minutes. %d / %d jobs completed.\n", completedCount, numJobs)
			fmt.Println("Some jobs may still be retrying or stuck — check job_runs for details.")
			break
		}

		time.Sleep(1 * time.Second)
	}

	fmt.Println("Cleaning up test jobs...")
	// delete job_runs first — jobs has a foreign key constraint from job_runs,
	// so the child rows must go before the parent rows
	_, err = db.Exec(`
		DELETE FROM job_runs 
		WHERE job_id IN (SELECT id FROM jobs WHERE name LIKE $1)
	`, runTag+"%")
	if err != nil {
		fmt.Println("Cleanup of job_runs failed:", err)
	}

	_, err = db.Exec(`DELETE FROM jobs WHERE name LIKE $1`, runTag+"%")
	if err != nil {
		fmt.Println("Cleanup of jobs failed (not critical):", err)
	} else {
		fmt.Println("Test jobs removed.")
	}
}

func printResults(numJobs int, elapsed time.Duration) {
	jobsPerMinute := float64(numJobs) / elapsed.Minutes()

	fmt.Println("===== LOAD TEST RESULTS =====")
	fmt.Printf("Total jobs:       %d\n", numJobs)
	fmt.Printf("Total time:       %.1f seconds\n", elapsed.Seconds())
	fmt.Printf("Throughput:       %.0f jobs/minute\n", jobsPerMinute)
	fmt.Println("==============================")
}