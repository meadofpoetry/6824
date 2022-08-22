package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unicode"
)

const (
	StatusIdle = iota
	StatusInProgress
	StatusComplete
)

type Job struct {
	Id          string
	Job         int
	Status      int
	MapArg      string
	ReduceArg   []string
	StartedAt   time.Time
}

func numericSuffix(s string) (int, error) {
	runes := []rune(s)
	var rev []rune
	
	for i := range runes {
		r := runes[len(runes)-i-1]
		if !unicode.IsDigit(r) {
			break
		}
		rev = append(rev, r)
	}

	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return strconv.Atoi(string(rev))
}

func (job *Job) ToBeDone() bool {
	if job.Status == StatusIdle {
		return true
	} else if job.Status == StatusInProgress {
		now := time.Now()
		duration := now.Sub(job.StartedAt)
		return duration >= time.Second * 10
	}

	return false
}

type Coordinator struct {
	lock        sync.Mutex
	jobs        []*Job
	reduceFiles [][]string
	nReduce     int
}

func (c *Coordinator) WithPendingJob(f func(*Job)) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, j := range c.jobs {
		if j.ToBeDone() {
			f(j)
			return
		}
	}
}

func (c *Coordinator) WithJob(id string, f func(*Job)) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, j := range c.jobs {
		if j.Id == id {
			f(j)
			return
		}
	}

	log.Fatalf("No jobs with id %s found", id)
}

func (c *Coordinator) AppendReduce(files []string) {	
	for _, fname := range files {
		reduceId, _ := numericSuffix(fname)
		c.reduceFiles[reduceId] = append(c.reduceFiles[reduceId], fname)
	}
	
	allMapsDone := true
	
	for _, j := range c.jobs {
		if j.Job == JobMap {
			allMapsDone = allMapsDone && j.Status == StatusComplete
		}
	}

	if !allMapsDone {
		return
	}

	// Else create reduce jobs
	for reduceId := 0; reduceId < c.nReduce; reduceId++ {
		job := &Job{
			Id : fmt.Sprint(reduceId),
			Job : JobReduce,
			Status : StatusIdle,
			ReduceArg : c.reduceFiles[reduceId],
		}
		c.jobs = append(c.jobs, job)
		
	}
}

func (c *Coordinator) Idle(args *IdleMessage, reply *AssignJob) error {
	log.Printf("Called idle\n")
	
	c.WithPendingJob(func(job *Job) {
		log.Printf("Found pending job %v\n", *job)
		
		if job != nil {
			job.Status = StatusInProgress
			job.StartedAt = time.Now()

			reply.Id = job.Id
			reply.Job = job.Job
			reply.MapArg = job.MapArg
			reply.ReduceArg = job.ReduceArg
			reply.NReduce = c.nReduce
		}
	})
	
	return nil
}

func (c *Coordinator) Complete(args *CompleteJob, reply *CompleteAccepted) error {
	log.Printf("Called complete\n")
	
	c.WithJob(args.Id, func(job *Job) {
		job.Status = StatusComplete

		if args.Job == JobMap {
			c.AppendReduce(args.MapResult)
		}
		
		reply.Success = true
	})
	
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	allComplete := true
	for _, j := range c.jobs {
		allComplete = allComplete && j.Status == StatusComplete
	}
	return allComplete
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var jobs []*Job

	for _, file := range files {
		job := &Job{
			Id : fmt.Sprintf("map-%s", filepath.Base(file)),
			Job : JobMap,
			Status : StatusIdle,
			MapArg : file,
		}
		jobs = append(jobs, job)
	}
	
	c := Coordinator{
		lock : sync.Mutex{},
		jobs : jobs,
		reduceFiles : make([][]string, nReduce),
		nReduce : nReduce,
	}

	log.Printf("Starting coordinator\n")

	c.server()
	return &c
}
