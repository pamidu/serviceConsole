package core

import (
	"duov6.com/objectstore/client"
	"encoding/json"
	"time"
)

type Downloader struct {
	Dispatcher *Dispatcher
}

func (d *Downloader) Start() {
	d.Dispatcher = newDispatcher()

	startDownloadTimer()
	downloadObjects(d)
}

func downloadObjects(d *Downloader) {

	nowTime := time.Now().Local()                             //current time
	addedtime := nowTime.Add(time.Duration(15 * time.Minute)) //add 15 minutes to current time
	formattedTime := addedtime.Format("20060102150405")       //formatted new time

	rawBytes, _ := client.Go("com.duosoftware.com", "schedule", "token").GetMany().ByQuerying("Timestamp:[* " + formattedTime + "]").Ok()

	executeObjects(d, rawBytes)
}

func startDownloadTimer(d *Downloader) { //call downloadObjects every 15 minutes
	c := time.Tick(15 * time.Minute)
	for now := range c {
		downloadObjects(d)
	}
}

func executeObjects(d *Downloader, raw []byte) {
	//unmarshall raw bytes to map[string]interface{}
	unmarshalObjects := make([]map[string]interface{})
	json.Unmarshal(raw, unmarshalObjects)
	d.Dispatcher.addObjects(unmarshalObjects)
}
