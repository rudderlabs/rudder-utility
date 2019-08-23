package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"strings"

	"strconv"

	"github.com/tidwall/sjson"
)

const (
	beURL                   string = "http://localhost:8080/hello"
	eventNamePos            int    = 4
	eventValuePos           int    = 5
	rudderEventMapping      string = "rl_message.rl_event"
	rudderEventPropsMapping string = "rl_message.rl_properties"
	rudderMsgID             string = "rl_message.rl_message_id"
	rudderMsgTimestamp      string = "rl_message.rl_timestamp"
	timeFormat              string = "2019-08-19 10:39:58+0000"
	batchSize               int    = 2
)

type Event struct {
	eventName  string
	eventValue map[string]interface{}
}

var (
	rudderEcommMessage []byte

	rudderNonEcommMessage []byte

	noOfMessages int

	rudderBatch []byte

	rudderEvent = []byte(`{ "batch": [] }`)

	eventNameMap = map[string]string{
		"af_list_view": "Product List Viewed",
	}

	eventValueMap = map[string]map[string]string{
		"Product List Viewed": map[string]string{
			"list_id":         "rl_message.rl_properties.list_id",
			"af_content_type": "rl_message.rl_properties.category",
			"af_content_list": "rl_message.rl_properties.products.sub.product_id",
			"sku":             "rl_message.rl_properties.products.sub.sku",
			"category":        "rl_message.rl_properties.products.sub.category",
			"name":            "rl_message.rl_properties.products.sub.name",
			"brand":           "rl_message.rl_properties.products.sub.brand",
			"price":           "rl_message.rl_properties.products.sub.price",
			"quantity":        "rl_message.rl_properties.products.sub.quantity",
			"coupon":          "rl_message.rl_properties.products.sub.quantity",
			"position":        "rl_message.rl_properties.products.sub.position",
			"url":             "rl_message.rl_properties.products.sub.url",
			"image_url":       "rl_message.rl_properties.products.sub.image_url",
		},
	}
)

func main() {
	fmt.Println("hello")
	var err error

	batchString := string(rudderEvent)
	rudderBatch = []byte(batchString)

	rudderEcommMessage, err = ioutil.ReadFile("sample-ecomm-rudder.json")
	if err != nil {
		panic(err)
	}

	rudderNonEcommMessage, err = ioutil.ReadFile("sample-nonecomm-rudder.json")
	if err != nil {
		panic(err)
	}

	readFile("sample-events.csv")
}

func readFile(fileName string) {
	csvFile, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)

	count := 0

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if count > 0 && len(row) > 0 {
			event := &Event{}
			if len(row[eventNamePos]) > 0 {
				event.eventName = row[eventNamePos]
			}
			if len(row[eventValuePos]) > 0 {
				err := json.Unmarshal([]byte(row[eventValuePos]), &event.eventValue)
				if err != nil {
					panic(err)
				}
			}
			var batch map[string]interface{}
			message := convertToRudderEvent(event)
			json.Unmarshal(message, &batch)
			if noOfMessages < batchSize {
				rudderBatch, _ = sjson.SetBytes(rudderBatch, "batch."+strconv.Itoa(noOfMessages), batch)
			} else {
				rudderBatch, _ = sjson.SetBytes(rudderBatch, "sent_at", time.Now().Format(timeFormat))
				sendToRudder(rudderBatch)
				batchString := string(rudderEvent)
				rudderBatch = []byte(batchString)
				noOfMessages = 0
			}
			noOfMessages++
			//fmt.Println(string(message))
		}
		count++
	}

	//rudderBatch, _ = sjson.SetBytes(rudderBatch, "sent_at", time.Now().Format(timeFormat))  //TODO
	if noOfMessages <= batchSize {
		sendToRudder(rudderBatch)
		batchString := string(rudderEvent)
		rudderBatch = []byte(batchString)
		noOfMessages = 0
	}

}

func convertToRudderEvent(ev *Event) []byte {
	var ok bool
	var rudderEventName string
	var message []byte

	if ev.eventName == "Page" || ev.eventName == "Screen" {
		messageStr := string(rudderNonEcommMessage)
		message = []byte(messageStr)
	} else {
		messageStr := string(rudderEcommMessage)
		message = []byte(messageStr)
	}

	//message, _ = sjson.SetBytes(message, rudderMsgID, uuid.NewV4().String())  (using a matching identifier instead)

	message, _ = sjson.SetBytes(message, rudderMsgID, ev.eventValue["rl_message_id"])
	delete(ev.eventValue, "rl_message_id")
	//message, _ = sjson.SetBytes(message, rudderMsgTimestamp, time.Now().Format(timeFormat))  //TODO
	rudderEventName, ok = eventNameMap[ev.eventName]
	if !ok {
		if !(ev.eventName == "Page" || ev.eventName == "Screen") {
			message, _ = sjson.SetBytes(message, rudderEventMapping, ev.eventName)
		}
		for k, v := range ev.eventValue {
			message, _ = sjson.SetBytes(message, rudderEventPropsMapping+"."+k, v)
		}
	} else {
		if !(ev.eventName == "Page" || ev.eventName == "Screen") {
			message, _ = sjson.SetBytes(message, rudderEventMapping, rudderEventName)
		}
		var pathFound bool
		var mappingPath interface{}
		for k, v := range ev.eventValue {
			//message, _ = sjson.SetBytes(message, k, v)

			mappingPath, pathFound = eventValueMap[rudderEventName][k]
			if !pathFound {
				message, _ = sjson.SetBytes(message, k, v)
			} else {
				if strings.Index(mappingPath.(string), "sub") >= 0 {
					for index, value := range v.([]interface{}) {
						message, _ = sjson.SetBytes(message, strings.Replace(mappingPath.(string), "sub", strconv.Itoa(index), -1), value)
					}
				} else {
					message, _ = sjson.SetBytes(message, mappingPath.(string), v)
				}
			}
		}
	}

	fmt.Println(string(message))
	return message
}

func sendToRudder(message []byte) {

	fmt.Println("===sending batch===", string(message))

	req, err := http.NewRequest("POST", beURL, bytes.NewBuffer(message))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))
}
