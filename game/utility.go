package main

// not pushing test files, when needed can add for tests locally

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
)

func main() {
	gameFile, err := os.Open("game7.json")
	if err != nil {
		fmt.Println("error opening file")
		return
	}
	rudderFile, errNew := os.Open("rudder7.json")
	if errNew != nil {
		fmt.Println("error opening file")
		return
	}

	reader := bufio.NewReader(gameFile)

	b, err := reader.ReadBytes('\n')

	// proccessed := 0
	leave := 0

	gameInsertIDToFound := make(map[string]bool)
	insertIDToEventPropertiesMap := make(map[string]map[string]interface{})
	var gameInserIDOrder []string
	var unmarhsalledData map[string]interface{}
	var propsNotMatch []string
	var inserIDNotPresent []string

	var dupIDs []string

	for err != io.EOF {
		/* if proccessed <= 120 {
			proccessed++
			b, err = reader.ReadBytes('\n')
			continue
		} */
		json.Unmarshal(b, &unmarhsalledData)
		eventProperties := unmarhsalledData["event_properties"].(map[string]interface{})
		insertID := eventProperties["rl_message_id"].(string)
		insertIDToEventPropertiesMap[insertID] = eventProperties
		_, found := gameInsertIDToFound[insertID]
		if found {
			dupIDs = append(dupIDs, insertID)
		}
		gameInsertIDToFound[insertID] = false
		gameInserIDOrder = append(gameInserIDOrder, insertID)
		leave++
		b, err = reader.ReadBytes('\n')
	}

	fmt.Println("game number of insertids: ", leave)
	fmt.Println("dups: ", len(dupIDs), dupIDs)

	reader.Reset(rudderFile)
	// proccessed = 0
	order := 0
	props := 0

	leave = 0

	b, err = reader.ReadBytes('\n')
	for err != io.EOF {
		/* if proccessed <= 135 {
			proccessed++
			b, err = reader.ReadBytes('\n')
			continue
		} */
		json.Unmarshal(b, &unmarhsalledData)
		eventProperties := unmarhsalledData["event_properties"].(map[string]interface{})
		insertID := eventProperties["rl_message_id"].(string)

		// see if insert id present in game
		_, ok := gameInsertIDToFound[insertID]
		if !ok {
			fmt.Println("insert id in rudder not found in game: ", insertID)
			inserIDNotPresent = append(inserIDNotPresent, insertID)
		} else {
			// see if insert id order match
			if gameInserIDOrder[leave] == insertID {
				// fmt.Println("insert id order match!!!")
				order++
			} else {
				fmt.Println("leave: ", leave)
				fmt.Println("insert id out of order: ", gameInserIDOrder[leave], " ", insertID)
			}

			gameInsertIDToFound[insertID] = true

			// see if event props match
			delete(insertIDToEventPropertiesMap[insertID], "category")
			if reflect.DeepEqual(eventProperties, insertIDToEventPropertiesMap[insertID]) {
				//fmt.Println("event props match: ", true)
				props++
			} else {
				//fmt.Println("event props match: ", false)
				propsNotMatch = append(propsNotMatch, insertID)

			}

		}
		leave++

		b, err = reader.ReadBytes('\n')
	}

	fmt.Println("order matched: ", order)
	fmt.Println("props matched: ", props)
	fmt.Println(" check these insertIds, props don't match: ", len(propsNotMatch), propsNotMatch)
	fmt.Println(" check these insertIds, ids not present in game: ", len(inserIDNotPresent), inserIDNotPresent)

	fmt.Println(" check there insertIds, ids not present in rudder ")

	notFound := 0
	for k, v := range gameInsertIDToFound {
		if !v {
			notFound++
			fmt.Println(k)
		}
	}
	fmt.Println(notFound)
}
