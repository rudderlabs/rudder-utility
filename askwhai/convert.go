package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/tidwall/sjson"
)

var (
	numbeOfUserChannels      = 10
	numberOfEventsPerChannel = 50
	filePath                 = "./data/input.json"
	dataPlaneURI             = "http://localhost:8090/v1/"
	writeKey                 = "1ZF0t47FxqCg2mGFfsHTblpp4bD"

	client = &http.Client{}
)

func assert(errored error) {
	if errored != nil {
		panic(errored)
	}
}

func getHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func sendToRudder(jsonPayload []byte, requestType string) {
	// making request to /track or /identify endpoint
	req, err := http.NewRequest("POST", dataPlaneURI+requestType, bytes.NewBuffer(jsonPayload))
	req.Close = true

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(writeKey, "")

	resp, err := client.Do(req)

	if resp != nil && resp.Body != nil {
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
		defer resp.Body.Close()
	}

	assert(err)
}

// transform AM event to a rudder payload
// prepopulated fields not specific to mapping such as app.build , library etc.
func transform(amEvent map[string]interface{}) ([]byte, string) {
	json := []byte(`{"channel":"web","context":{"app":{"build":"1.0.0","name":"RudderLabs JavaScript SDK","namespace":"com.rudderlabs.javascript","version":"1.1.1-rc.0"},"library":{"name":"RudderLabs JavaScript SDK","version":"1.1.1-rc.0"},"userAgent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36","locale":"en-US","os":{"name":"","version":""},"screen":{"density":1.7999999523162842}},"type":"","messageId":"","originalTimestamp":"2020-03-16T12:55:20.565Z","anonymousId":"","userId":"","event": "","integrations":{"All":true},"sentAt":""}`)

	var requestType string

	mappedEventProp, _ := amEvent["event_properties"].(map[string]interface{})

	mappedUserPro, _ := amEvent["user_properties"].(map[string]interface{})

	// checking for identify events, seems like AM export data for askwhai don't have one
	if mappedEventProp == nil {
		json, _ = sjson.SetBytes(json, "type", "identify")
		requestType = "identify"
	} else {
		requestType = "track"
		json, _ = sjson.SetBytes(json, "type", "track")
		json, _ = sjson.SetBytes(json, "event", amEvent["event_type"])
	}
	json, _ = sjson.SetBytes(json, "properties", mappedEventProp)
	json, _ = sjson.SetBytes(json, "context.traits", mappedUserPro)
	json, _ = sjson.SetBytes(json, "userProperties", mappedUserPro) //safegueard transformer
	json, _ = sjson.SetBytes(json, "originalTimestamp", time.Now().Format("2006-01-02T15:04:05Z"))

	addOtherTransformation(&json, amEvent)

	return json, requestType
}

func addOtherTransformation(outputJSON *[]byte, unmarhsalledSourceData map[string]interface{}) {
	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.traits.address.city", unmarhsalledSourceData["city"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.traits.address.country", unmarhsalledSourceData["country"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "userProperties.city", unmarhsalledSourceData["city"]) // safeguard transformer as not making identify calls

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "userProperties.country", unmarhsalledSourceData["country"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.ip", unmarhsalledSourceData["ip_address"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.os.name", unmarhsalledSourceData["os_name"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.os.version", unmarhsalledSourceData["os_version"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.device.id", unmarhsalledSourceData["device_id"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.device.name", unmarhsalledSourceData["device_brand"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.device.model", unmarhsalledSourceData["device_model"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.device.manufacturer", unmarhsalledSourceData["device_manufacturer"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "context.network.carrier", unmarhsalledSourceData["device_carrier"])

	//*outputJSON, _ = sjson.SetBytes(*outputJSON, "integrations", []string{"amplitude"})  // making integration as All:true

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "anonymousId", unmarhsalledSourceData["user_id"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "userId", unmarhsalledSourceData["user_id"])

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "session_id", strconv.Itoa(int(unmarhsalledSourceData["session_id"].(float64)))) // passing session from the AM export data

	*outputJSON, _ = sjson.SetBytes(*outputJSON, "messageId", unmarhsalledSourceData["$insert_id"])

}

func execute(userChannel chan map[string]interface{}, done chan bool, completed chan bool) {
	for {
		select {
		case event := <-userChannel:
			fmt.Println("recieved")
			// time.Sleep(100 * time.Millisecond) // for test
			outputJSONEvent, requestType := transform(event)
			outputJSONEvent, _ = sjson.SetBytes(outputJSONEvent, "sentAt", time.Now().Format("2006-01-02T15:04:05Z"))
			// fmt.Println(string(outputJSONEvent))
			// fmt.Println(requestType)
			sendToRudder(outputJSONEvent, requestType)

		case <-done:
			fmt.Println("done...")
			completed <- true
			break

		}
	}

}

func main() {

	// take console input for relevant fields, defaults for testing
	numbeOfUserChannels = *flag.Int("numbeOfUserChannels", 10, "an int")
	numberOfEventsPerChannel = *flag.Int("numberOfEventsPerChannel", 50, "an int")
	filePath = *flag.String("filePath", "./data/input.json", "a string")
	dataPlaneURI = *flag.String("dataPlaneURI", "http://localhost:8090/v1/", "a string")
	writeKey = *flag.String("writeKey", "1ZF0t47FxqCg2mGFfsHTblpp4bD", "an int")

	userChannels := make([]chan map[string]interface{}, numbeOfUserChannels)
	done := make(chan bool)
	completed := make(chan bool)

	// create user-channels responsible for handling events for a set for users
	// each channel capacity being equal to numberOfEventsPerChannel
	for i := 0; i < numbeOfUserChannels; i++ {
		userChannels[i] = make(chan map[string]interface{}, numberOfEventsPerChannel)
		go execute(userChannels[i], done, completed)
	}

	var AMEventDict map[string]interface{}
	AMEventDict = nil

	f, err := os.Open(filePath)
	assert(err)
	reader := bufio.NewReader(f)

	// json events deliminated by new line
	jsonLine, err := reader.ReadBytes('\n')

	for err != io.EOF {
		// fmt.Println(string(jsonLine))
		AMEventDict = nil
		errored := json.Unmarshal(jsonLine, &AMEventDict)
		assert(errored)

		// bucket users by their hashed user-ids
		hashedUserID := getHash(AMEventDict["user_id"].(string)) % numbeOfUserChannels
		userChannels[hashedUserID] <- AMEventDict

		jsonLine, err = reader.ReadBytes('\n')
	}

	// notify user-channels that no further events are remaining
	for i := 0; i < numbeOfUserChannels; i++ {
		done <- true
	}

	// end program gracefully
	for i := 0; i < numbeOfUserChannels; i++ {
		<-completed
	}

}
