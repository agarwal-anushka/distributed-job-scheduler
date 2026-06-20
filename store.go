package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type Job struct {
	ID       int
	Name     string
	CronExpr string
	Status   string
	JobType  string
}

type JobRun struct {
	ID        int
	JobID     int
	JobName   string
	JobType   string
	Status    string
	Attempt   int
	CreatedAt time.Time
}

func createJob(db *sql.DB, name string, cronExpr string) error {
    if err := validateCronExpr(cronExpr); err != nil {
        return fmt.Errorf("invalid cron expression: %w", err)
    }
    _, err := db.Exec(
        "INSERT INTO jobs (name, cron_expr) VALUES ($1, $2)",
        name, cronExpr,
    )
    return err
}

func getJobs(db *sql.DB) ([]Job, error) {
    rows, err := db.Query("SELECT id, name, cron_expr, status, job_type FROM jobs")
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var jobs []Job
    for rows.Next() {
        var j Job
        if err := rows.Scan(&j.ID, &j.Name, &j.CronExpr, &j.Status, &j.JobType); err != nil {
            fmt.Println("scan error:", err)
            continue
        }
        jobs = append(jobs, j)
    }
    return jobs, nil
}

func createRun(db *sql.DB, jobID int) (int, error) {
	var id int
	err := db.QueryRow(
		"INSERT INTO job_runs (job_id, status) VALUES ($1, 'pending') RETURNING id",
		jobID,
	).Scan(&id)
	return id, err
}

func updateRunStatus(db *sql.DB, runID int, status string) error {
	_, err := db.Exec(
		"UPDATE job_runs SET status=$1, finished_at=$2 WHERE id=$3",
		status, time.Now(), runID,
	)
	return err
}

func getJobsDue(db *sql.DB) ([]Job, error) {
	rows, err := db.Query(
		"SELECT id, name, cron_expr, status, job_type FROM jobs WHERE next_run_at <= NOW() AND status = 'active'",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Name, &j.CronExpr, &j.Status, &j.JobType); err != nil {
			fmt.Println("scan error:", err)
			continue
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func updateNextRun(db *sql.DB, jobID int, nextRun time.Time) error {
	_, err := db.Exec(
		"UPDATE jobs SET next_run_at=$1 WHERE id=$2",
		nextRun, jobID,
	)
	return err
}

func validateCronExpr(cronExpr string) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(cronExpr)
	return err
}

func incrementRetry(db *sql.DB, jobID int) (int, error) {
	var retryCount int
	err := db.QueryRow(`
        UPDATE jobs 
        SET retry_count = retry_count + 1 
        WHERE id = $1 
        RETURNING retry_count`,
		jobID,
	).Scan(&retryCount)
	return retryCount, err
}

func getMaxRetries(db *sql.DB, jobID int) (int, error) {
	var maxRetries int
	err := db.QueryRow(
		"SELECT max_retries FROM jobs WHERE id = $1",
		jobID,
	).Scan(&maxRetries)
	return maxRetries, err
}

func getStuckRuns(db *sql.DB) ([]JobRun, error) {
	rows, err := db.Query(`
        SELECT jr.id, jr.job_id, j.name, jr.status, j.job_type
        FROM job_runs jr
        JOIN jobs j ON j.id = jr.job_id
        WHERE jr.status = 'pending'
        AND jr.created_at < NOW() - INTERVAL '5 minutes'
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []JobRun
	for rows.Next() {
		var r JobRun
		if err := rows.Scan(&r.ID, &r.JobID, &r.JobName, &r.Status, &r.JobType); err != nil {
			fmt.Println("scan error:", err)
			continue
		}
		runs = append(runs, r)
	}
	return runs, nil
}
