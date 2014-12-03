package core

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type Dispatcher struct {
	PendingObjects map[string]interface{}
	ScheduleTable  ScheduleTable
}

type TableRow struct {
	Timestamp string
	Objects   []map[string]interface{}
}

type ScheduleTable struct {
	Rows []TableRow
}

func (d *Dispatcher) addObjects(objects []map[string]interface{}) {
	for _, element := range objects {
		d.ScheduleTable.InsertObject(element)
	}
}

func (t *ScheduleTable) Get(timestamp string) (obj map[string]interface{}) {
	if t.Contains(timestamp) == true {
		return t.Rows[timestamp]
	} else {
		return nil
	}
}

func (t *ScheduleTable) InsertObject(obj map[string]interface{}) {
	timestamp := obj["Timestamp"]
	var currentTableRow TableRow
	for tableRow := range t.Rows {
		if tableRow.TimeStamp == timestamp {
			currentTableRow = tableRow
			break
		}
	}

	if currentTableRow == nil {
		currentTableRow = TableRow{}
		currentTableRow.Timestamp = timestamp

		var maps []map[string]interface{}
		maps = make([]map[string]interface{}, 1)
		maps[0] = obj
		currentTableRow.Objects = maps

		t.AddRow(currentTableRow)
	} else {
		var objects []map[string]interface{} = currentTableRow.Objects

	}

}

func (t *ScheduleTable) AddRow(row *TableRow) {
	tablesize := len(t.Rows)
	t.Rows[tablesize].Timestamp = row.Timestamp
	t.Rows[tablesize].Timestamp = row.Objects
}

func (t *ScheduleTable) Contains(timestamp string) bool {
	var currentTableRow TableRow
	for rows := range currentTableRow {
		if rows["Timestamp"] == timestamp {
			return true
		}
	}
	return false

}

func (t *ScheduleTable) Delete(timestamp string) {

	index := -1

	for i, element := range t.Rows {
		if element.Timestamp == timestamp {

			nextrow := obj + 1
			for nextrow, _ := range t.Rows {
				t.Rows[obj].Timestamp = t.Rows[nextrow].Timestamp
				t.Rows[obj].Objects = t.Rows[nextrow].Objects
			}

			index = i
			break
		}
	}

	if index != -1 {
		if index == 0 {
			t.Rows = t.Rows[1:0]
		} else {
			t.Rows = append(t.Rows[0:index-1], t.Rows[index:])
		}

	}

	/*	for obj := range t.Rows {
		if t.Rows[obj].Timestamp == timestamp {
			nextrow := obj + 1
			for nextrow, _ := range t.Rows {
				t.Rows[obj].Timestamp = t.Rows[nextrow].Timestamp
				t.Rows[obj].Objects = t.Rows[nextrow].Objects
			}
			break
		}

	}*/

}

func (t *ScheduleTable) GetForExecution(timestamp string) (row *TableRow) {
	for rows := range t.Rows {
		if t.Rows[rows].Timestamp == timestamp {
			objects := t.Rows[rows].Objects
			dispatchObjectToRabbitMQ(objects)
		}
	}

}

func newDispatcher() (d *Dispatcher) {
	newObj := Dispatcher{}
	newObj.ScheduleTable = ScheduleTable{}
	newObj.PendingObjects = make([]map[string]interface{}, 1)
	startDispatchTimer()
	return &newObj

}

func (t *ScheduleTable) startDispatchTimer() { //objetcs []map[string]interface{}) { //set timer interval for 1 second
	c := time.Tick(1 * time.Second)
	for now := range c {
		//get a timestamp of now
		currenttime := time.Now().Local()
		x := currenttime.Format("20141212101112")
		objectlists := t.Rows.Get(x)
		if x != nil {
			dispatchObjectToRabbitMQ(objectlists)
			t.Delete(x)
		}

	}

}

func dispatchObjectToRabbitMQ(objects []map[string]interface{}) {
	objectsset := getFakeObjects()
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"DuoRabbitMq", // name
		false,         // durable
		false,         // delete when usused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)

	failOnError(err, "Failed to declare a queue")

	for ob := range objects {

		sort.Strings(objects)

		for transfer := range objects {
			dataset, _ := json.Marshal(objects[transfer])
			body := dataset
			err = ch.Publish(
				" ",    // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				})
		}
	}

	failOnError(err, "Failed to publish a message")

}

func getFakeObjects() (objects []ScheduleObject) {

	objects = make([]ScheduleObject, 1)

	for index, _ := range objects {

		var tmpOperationData map[string]interface{}
		tmpOperationData = make(map[string]interface{})
		tmpOperationData["a"] = 1
		tmpOperationData["b"] = 2
		tmpOperationData["g"] = 9
		tmpOperationData["h"] = 10
		tmpOperationData["i"] = 11
		tmpOperationData["j"] = 22
		tmpOperationData["k"] = 13
		tmpOperationData["l"] = 24

		var tmpControlData map[string]interface{}
		tmpControlData = make(map[string]interface{})
		tmpControlData["a"] = 3
		tmpControlData["b"] = 4
		tmpControlData["c"] = 5
		tmpControlData["d"] = 6
		tmpControlData["e"] = 7
		tmpControlData["f"] = 8

		t := "20141124175827"        //time.Now().Local()
		objects[index].Timestamp = t //.Format("20060102150405")
		objects[index].OperationData = tmpOperationData
		objects[index].ControlData = tmpControlData

	}

	return objects

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
