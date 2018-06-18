package main

import (
	"github.com/gorilla/mux"
	"log"
	_ "github.com/go-sql-driver/mysql"
	"net/http"
	"database/sql"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"time"
	"sync"
)

var DB *sql.DB

func initDb() {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/hotel_booking")
	if err != nil {
		fmt.Println(err.Error())
	}
	if err != nil {
		fmt.Println(err.Error())
	}
	db.SetMaxOpenConns(100)
	DB = db
}

type User struct {
	Id   int   `json:"id,omitempty"`
	Name string   `json:"name,omitempty"`
}
type HotelBooking struct {
	Id          int   `json:"id,omitempty"`
	UserId      int   `json:"user_id,omitempty"`
	BookingDate string   `json:"booking_date,omitempty"`
}
type SyncData struct {
	Users []User           				 `json:"users"`
	HotelBookings []HotelBooking           `json:"hotel_bookings"`
}

// our main function
func main() {
	initDb()
	router := mux.NewRouter()
	router.HandleFunc("/", HelloWorld).Methods("GET")
	router.HandleFunc("/sync", Sync).Methods("POST")
	log.Fatal(http.ListenAndServe("0.0.0.0:8000", router))
}


func HelloWorld(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello World")
}
func Sync(w http.ResponseWriter, r *http.Request) {
	var wg sync.WaitGroup
	timeStart := time.Now()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println(err)
	}
	log.Println(string(body))
	//persisting to db
	syncData := new(SyncData)
	json.Unmarshal([]byte(body), &syncData)
	for _, element := range syncData.Users {
		wg.Add(1)
		go saveUserInDb(element, &wg)
	}
	for _, element := range syncData.HotelBookings {
		wg.Add(1)
		go savehotelBookingInDb(element, &wg)
	}
	wg.Wait()
	elaspsed := time.Now().Sub(timeStart)
	log.Println(elaspsed)
	fmt.Fprintf(w, "processed ", int64(elaspsed/time.Nanosecond))
}

//function used by go routine
//to save user in db
func saveUserInDb(user User, wg *sync.WaitGroup) {
	stmt, err := DB.Prepare("insert into users (name) values(?);")
	if err != nil {
		fmt.Print(err.Error())
	}
	_, err = stmt.Exec(user.Name)
	if err != nil {
		fmt.Print(err.Error())
	}
	wg.Done()
}

//function used by go routine
//to save hotelbooking in db
func savehotelBookingInDb(hotelBooking HotelBooking, wg *sync.WaitGroup) {
	stmt, err := DB.Prepare("insert into hotel_bookings (user_id, booking_date) values(?,?);")
	if err != nil {
		fmt.Print(err.Error())
	}
	_, err = stmt.Exec(hotelBooking.UserId, hotelBooking.BookingDate)
	if err != nil {
		fmt.Print(err.Error())
	}
	wg.Done()
}

