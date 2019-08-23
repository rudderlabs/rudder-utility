package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	eventNamePos  int = 4
	eventValuePos int = 5
)

var (
	eventFound = make(map[string]bool)
	eventMap   = make(map[string][]string)

	eventDiscrepantMap = make(map[string][]string)
)

func main() {
	fmt.Println("hello")

	csvFileFirst, err := os.Open("sample-events1.csv")
	if err != nil {
		panic(err)
	}
	defer csvFileFirst.Close()

	csvFileSecond, err := os.Open("sample-events2.csv")
	if err != nil {
		panic(err)
	}
	defer csvFileSecond.Close()

	firstReader := csv.NewReader(csvFileFirst)
	secondReader := csv.NewReader(csvFileSecond)

	count := 0
	for {
		row, err := firstReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if count > 0 {
			var eventValue map[string]interface{}
			json.Unmarshal([]byte(row[eventValuePos]), &eventValue)
			//fmt.Println("==eventValue===", eventValue)
			eventMap[eventValue["rl_message_id"].(string)] = []string{row[eventNamePos], row[eventValuePos]}
			eventFound[eventValue["rl_message_id"].(string)] = false
		}

		count++

	}

	count = 0

	for {
		row, err := secondReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if count > 0 {
			var eventValue map[string]interface{}
			json.Unmarshal([]byte(row[eventValuePos]), &eventValue)

			eventDetails, ok := eventMap[eventValue["rl_message_id"].(string)]

			if !ok {
				eventDiscrepantMap[eventValue["rl_message_id"].(string)] = []string{row[eventNamePos], row[eventValuePos]}
			} else {
				eventFound[eventValue["rl_message_id"].(string)] = true
				if row[eventNamePos] != eventDetails[0] || row[eventValuePos] != eventDetails[1] {
					eventDiscrepantMap[eventValue["rl_message_id"].(string)] = []string{row[eventNamePos], row[eventValuePos]}
				}
			}
		}
		count++

	}

	fmt.Println("=====discrepancies in second file====", len(eventDiscrepantMap))
	for k, v := range eventDiscrepantMap {
		fmt.Println("messageID:: ", k)
		fmt.Println(v)
	}

	fmt.Println("=====discrepancies in first file====")
	for k, v := range eventFound {
		if !v {
			fmt.Println("messageID:: ", k)
			fmt.Println(eventMap[k])
		}
	}

}
