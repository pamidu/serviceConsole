package main

import (
	"duov6.com/serviceconsole/scheduler/core"
)

type Worker struct {
}

func (w *Worker) Start() {
	downloader := core.Downloader{}
	downloader.Start()
}

func main() {
	worker := Worker{}
	worker.Start()
}
