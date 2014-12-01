package core

import (
	"fmt"
	"github.com/streadway/amqp"
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
		//timeStamp := element["Timestamp"]
		d.ScheduleTable.InsertObject(element)
		//fmt.Println(timeStamp)
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
	t.Rows[tablesize].Objects = row.Objects

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

func (t *ScheduleTable) GetForExecution(timestamp string) (row *TableRow) {
	for rows := range t.Rows {
		if t.Rows[rows].Timestamp == timestamp {
			objects := t.Rows[rows].Objects
			dispatchObjectToRabbitMQ(objects)
		}
	}

}

/////////////////////////////////////////////////////////////////////////////////////////////////

func newDispatcher() (d *Dispatcher) {
	newObj := Dispatcher{}
	newObj.ScheduleTable = ScheduleTable{}
	newObj.PendingObjects = make([]map[string]interface{}, 1)
	startDispatchTimer()
	return &newObj

}

func startDispatchTimer() { //set timer interval for 1 second
	c := time.Tick(1 * time.Second)
	for now := range c {
		dispatchObjectToRabbitMQ(objects)
	}

}

func dispatchObjectToRabbitMQ(objects []map[string]interface{}) {
	//get objects
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

		t := time.Now().Local()
		abc := t.Format("20060102150405")

		currentTimeInt, err := strconv.Atoi(abc)
		if err != nil {
			fmt.Println(err)

		}

		sort.Strings(objects)

		for transfer := range objects {
			dataset, _ := json.Marshal(objects[transfer])
			body := dataset
			err = ch.Publish(
				"",     // exchange
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

	objects = make([]ScheduleObject, 5)

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
		//fmt.Println(objects[index], "\n")

	}

	return objects

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
