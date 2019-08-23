package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

var (
	eventValueMap = map[string]interface{}{
		"_eventName":      "fb_mobile_purchase",
		"fb_content_type": "product",
		"_valueToSum":     21.97,
		"fb_currency":     "USD",
	}

	fbContent = map[string]interface{}{
		"id":       "3456",
		"quantity": 4,
	}
)

func main() {

	/* events := []byte(`[{"_eventName":"fb_mobile_purchase",
		"fb_content":"[{"id": "1234", "quantity": 2,}, {"id": "5678", "quantity": 1,}]",
		"fb_content_type":"product",
		"_valueToSum":21.97,
		"fb_currency":"GBP",
	  }]`) */

	var fbContentArray []map[string]interface{}

	var eventsAraay []map[string]interface{}

	fbContentArray = append(fbContentArray, fbContent)

	val, _ := json.Marshal(fbContentArray)

	eventValueMap["fb_content"] = string(val)

	eventsAraay = append(eventsAraay, eventValueMap)

	eventString, _ := json.Marshal(eventsAraay)

	postURL := "https://graph.facebook.com/v3.3/644758479345539/activities?access_token=644758479345539|748924e2713a7f04e0e72c37e336c2bd"

	urlData := url.Values{}
	urlData.Set("event", "CUSTOM_APP_EVENTS")

	urlData.Set("advertiser_id", "1111-1111-1111-1111")

	urlData.Set("advertiser_tracking_enabled", "1")

	urlData.Set("application_tracking_enabled", "1")

	urlData.Set("custom_events", string(eventString))

	resp, err := http.PostForm(postURL, urlData)

	if err != nil {
		fmt.Println(err)
	}

	defer resp.Body.Close()
	respBody, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(respBody))

	fmt.Println("Status : ", resp.Status)
	fmt.Println("Header: ", resp.Header)
	//fmt.Println("Body: ", resp.Body)

}
