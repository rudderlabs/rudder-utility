package appsflyer

type deviceOS string

const (
	Android deviceOS = "android"
	IOS     deviceOS = "ios"
)

type EventName string

const (

	//Entertainment

	StartTrial EventName = "af_start_trial"
	Subscribe  EventName = "af_subscribe"

	// Gaming

	Login         EventName = "af_login"
	LevelAchieved EventName = "af_level_achieved"
	BonusClaimed  EventName = "bonus_claimed"

	//ECommerce

	RegistrationMethod EventName = "af_complete_registration"
	ProductSearch      EventName = "af_search"
	ProductViewed      EventName = "af_content_view"
	ListViewed         EventName = "af_list_view"
	AddToWishList      EventName = "af_add_to_wishlist"
	AddToCart          EventName = "af_add_to_cart"
	InitiateCheckout   EventName = "af_initiated_checkout"
	Purchase           EventName = "af_purchase"
	CompletedPurchase  EventName = "completed_purchase" //use if Purchase event not sent
	RemoveFromCart     EventName = "remove_from_cart"
	FirstPurchase      EventName = "first_purchase"

	//Fintech

	Payment          EventName = "payment"
	PaymentCompleted EventName = "payment_completed"

	//Navigation

	BookingRequested EventName = "ride_booking_requested"

	//Travel

	TravelBooking EventName = "af_travel_booking"
)

type EventParam string

const (
	ParamRevenue  EventParam = "af_revenue"
	ParamPrice    EventParam = "af_price"
	ParamCurrency EventParam = "af_currency"

	//Gaming
	ParamLevel     EventParam = "af_level"
	ParamScore     EventParam = "af_score"
	ParamBonusType EventParam = "bonus_type"

	//EComm
	ParamRegMethod           EventParam = "af_registration_method"
	ParamSearchString        EventParam = "af_search_string"
	ParamSearchOrContentList EventParam = "af_content_list"
	ParamContentID           EventParam = "af_content_id"
	ParamContentType         EventParam = "af_content_type"
	ParamQuantity            EventParam = "af_quantity"
	ParamOrderID             EventParam = "af_order_id"
	ParamRecieptID           EventParam = "af_receipt_id"

	//Navigation
	ParamStartDest EventParam = "af_destination_a"
	ParamEndDest   EventParam = "af_destination_b"
	ParamCity      EventParam = "af_city"
	ParamRegion    EventParam = "af_region"
	ParamCountry   EventParam = "af_country"

	//Travel
	ParamDepartureDate EventParam = "af_departing_departure_date"
	ParamReturnDate    EventParam = "af_returning_departure_date"
	ParamClass         EventParam = "af_class"
)
