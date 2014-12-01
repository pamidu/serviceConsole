package main

import (
	"duov6.com/serviceconsole/scheduler/core"
)

type Scheduler struct {
}

func (s *Scheduler) Start() {
	downloader := core.Downloader{}
	downloader.Start()
}

func main() {
	scheduler := Scheduler{}
	scheduler.Start()
}
