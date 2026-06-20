package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func startScheduler(db *sql.DB, rdb *redis.Client, session *concurrency.Session) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	fmt.Println("Scheduler started — checking every 10 seconds...")

	for {

		// check if our etcd session is still alive
        // if not — we lost leadership, stop immediately
        select {
        case <-session.Done():
            fmt.Println("Lost leadership — stepping down")
            return
        default:
            // session still alive, keep going
        }


		// get all jobs that are due
		jobs, err := getJobsDue(db)
		if err != nil {
			fmt.Println("Error fetching jobs:", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if len(jobs) == 0 {
			fmt.Println("No jobs due right now...")
		}

		for _, job := range jobs {
			// check session again before each job — extra safety
            select {
            case <-session.Done():
                fmt.Println("Lost leadership mid-scheduling — stopping")
                return
            default:
            }

			fmt.Printf("Scheduling job: %s\n", job.Name)
			publishJob(rdb, job.ID, job.Name, job.JobType)

			schedule, err := parser.Parse(job.CronExpr)
			if err != nil {
				fmt.Println("Bad cron expression, pausing job:", job.Name)
				db.Exec("UPDATE jobs SET status='paused' WHERE id=$1", job.ID)
				continue
			}
			nextRun := schedule.Next(time.Now())
			updateNextRun(db, job.ID, nextRun)
		}

		// wait 10 seconds before checking again
		time.Sleep(10 * time.Second)
	}
}
