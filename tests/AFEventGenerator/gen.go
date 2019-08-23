package main

import (
	"time"

	af "github.com/rudderlabs/rudder-utility/tests/appsflyer"
	uuid "github.com/satori/go.uuid"
)

const (
	appsflyerID string = "1566196073242-3081226262989399620"
	advID       string = "f5f02c77-921e-4b36-9b3e-d14df4670b6f"
	devIP       string = "1.2.3.4"
	currency    string = "USD"
)

func main() {
	tracker := af.NewTracker()
	/* if trackErr != nil {
		panic(trackErr)
	} */
	tracker.SetConfig("appsflyer.json")

	startGamingEvents(tracker)
	startSubscriptionEvents(tracker)
	startEcommEvents(tracker)
	startFintechEvents(tracker)
	startNavigationEvents(tracker)
	startTravelEvents(tracker)
}

func startGamingEvents(tracker *af.Tracker) {
	login(*tracker)
	levelAchieved(*tracker)
	bonusClaimed(*tracker)
}

func startSubscriptionEvents(tracker *af.Tracker) {
	startTrial(*tracker)
	subscribe(*tracker)
	cancelSubscription(*tracker)
}

func startEcommEvents(tracker *af.Tracker) {
	completeRegistration(*tracker)
	login(*tracker)
	search(*tracker)
	contentView(*tracker)
	listView(*tracker)
	addToWishList(*tracker)
	addToCart(*tracker)
	initiateCheckout(*tracker)
	purchase(*tracker)
	removeFromCart(*tracker)
	firstPurchase(*tracker)
}

func startFintechEvents(tracker *af.Tracker) {
	payment(*tracker)
	paymentCompleted(*tracker)
}

func startNavigationEvents(tracker *af.Tracker) {
	bookingRequest(*tracker)
}

func startTravelEvents(tracker *af.Tracker) {
	travelBooking(*tracker)
}

func login(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android).
		SetName(af.Login).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func levelAchieved(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android).
		SetName(af.LevelAchieved).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).
		SetEventTime(time.Now()).
		SetValueInt(af.ParamLevel, 3).
		SetValueInt(af.ParamScore, 100).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func bonusClaimed(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android).
		SetName(af.BonusClaimed).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).
		SetEventTime(time.Now()).
		SetValue(af.ParamBonusType, "coins").SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func startTrial(tracker af.Tracker) {

	// User starts a trial
	evt := af.NewEvent(appsflyerID, af.Android).
		SetName(af.StartTrial).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).
		SetPrice(59.99, currency).
		SetDateValue("expiry", time.Now()).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func subscribe(tracker af.Tracker) {

	// User ends trial and pays for first subscription period
	evt := af.NewEvent(appsflyerID, af.Android).
		SetName(af.Subscribe).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).
		SetRevenue(59.99, currency).
		SetDateValue("expiry", time.Now()).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func cancelSubscription(tracker af.Tracker) {

	// User cancels a subscription
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName("cancel_subscription"))
	evt.SetRevenue(-59.99, "USD").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func completeRegistration(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.RegistrationMethod)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetValue(af.ParamRegMethod, "Google").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func search(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.ProductSearch)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetValue(af.ParamSearchString, "Mobiles").SetValueInterface(af.ParamSearchOrContentList, []string{"Redmi", "iPhone"}).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func contentView(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.ProductViewed)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetPrice(50.00, "USD").SetValue(af.ParamContentID, "MOB_123").SetValue(af.ParamContentType, "Mobiles").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func listView(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.ListViewed)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetValue(af.ParamContentType, "Mobiles").SetValueInterface(af.ParamSearchOrContentList, []string{"MOB_123", "MOB_345	"}).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func addToWishList(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.AddToWishList)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetPrice(150.00, "USD").SetValue(af.ParamContentID, "MOB_123").SetValue(af.ParamContentType, "Mobiles").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func addToCart(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.AddToCart)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetPrice(24.99, "USD").SetValue(af.ParamContentID, "MOB_123").
		SetValueInt(af.ParamQuantity, 1).
		SetValue(af.ParamContentType, "Mobiles").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func initiateCheckout(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.InitiateCheckout)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetPrice(400.00, "USD").SetValueInterface(af.ParamContentID, []string{"MOB_123", "MOB_345"}).
		SetValueInterface(af.ParamQuantity, []float64{1, 3}).
		SetValueInterface(af.ParamContentType, []string{"Mobiles", "Mobiles"}).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func purchase(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.Purchase)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetPrice(200.00, "USD").
		SetRevenue(400.00, "USD").SetValueInterface(af.ParamContentID, []string{"MOB_123", "MOB_345"}).
		SetValueInterface(af.ParamQuantity, []float64{1, 3}).
		SetValueInterface(af.ParamContentType, []string{"Mobiles", "Mobiles"}).SetValue(af.ParamOrderID, "Order_1").SetValue(af.ParamRecieptID, "RE_1").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func removeFromCart(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.RemoveFromCart)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetValue(af.ParamContentID, "MOB_123").SetValue(af.ParamContentType, "Mobiles").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func firstPurchase(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.FirstPurchase)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetPrice(200.00, "USD").
		SetRevenue(400.00, "USD").SetValueInterface(af.ParamContentID, []string{"MOB_123", "MOB_345"}).
		SetValueInterface(af.ParamQuantity, []float64{1, 3}).
		SetValueInterface(af.ParamContentType, []string{"Mobiles", "Mobiles"}).SetValue(af.ParamOrderID, "Order_1").SetValue(af.ParamRecieptID, "RE_1").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func payment(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.Payment)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetValue("loan_id", "LOAN_1").SetValueInterface("payment_amount", 200.50).SetValue("payment_id", "PAY_1").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func paymentCompleted(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.PaymentCompleted)).
		SetAdvertisingID(advID).
		SetDeviceIP(devIP).SetValue("loan_id", "LOAN_1").
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func bookingRequest(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.BookingRequested)).
		SetAdvertisingID(advID).
		SetPrice(150.50, "USD").SetValue(af.ParamStartDest, "Nagerbazar").
		SetValue(af.ParamEndDest, "ChinarPark").SetValue(af.ParamContentType, "Taxi").
		SetValue(af.ParamCity, "Kolkata").SetValue(af.ParamRegion, "East").SetValue(af.ParamCountry, "IN").
		SetDeviceIP(devIP).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}

func travelBooking(tracker af.Tracker) {
	evt := af.NewEvent(appsflyerID, af.Android)
	evt.SetName(af.EventName(af.TravelBooking)).
		SetAdvertisingID(advID).
		SetRevenue(300.00, "USD").
		SetPrice(150.50, "USD").
		SetValue(af.ParamContentID, "Hotel Taj").SetDateValueFlight(af.ParamDepartureDate, time.Now()).
		SetDateValueFlight(af.ParamReturnDate, time.Now()).
		SetValue(af.ParamStartDest, "CCU").
		SetValue(af.ParamEndDest, "DEL").SetValue(af.ParamClass, "Economy").SetValue("airline_code", "INDIGO").
		SetDeviceIP(devIP).
		SetEventTime(time.Now()).SetValue("rl_message_id", uuid.NewV4().String())

	if err := tracker.Send(evt); err != nil {
		panic(err)
	}
}
