package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

func main() {
	fmt.Println("hello")
	//mainFunc("../../../torpedo-data/torpedo1/0.json", "test.json", "nouser.json")

	//var files []string
	//readDir("../../../torpedo-data/torpedo1", files)

	compare = false
	batchSize = 10
	rudderSend = true
	noOfUsers = 100000
	noOfBatches = 20000

	//userIDToChannel = make(map[string]chan []byte)

	client = &http.Client{}

	db = pg.Connect(&pg.Options{
		User:     "ubuntu",
		Password: "ubuntu",
		Database: "testuserevent", //"testuserevent", //"userevent",
		Addr:     "localhost:5432",
	})

	if db != nil {
		fmt.Println("CONNECTED TO DB")
		defer db.Close()
	} else {
		fmt.Println("CONNECTION ERROR")
		return
	}

	if !compare {
		opts := &orm.CreateTableOptions{
			IfNotExists: true,
		}

		crErr := db.CreateTable(&userEventList{}, opts)

		if crErr != nil {
			panic(crErr)
		}
	}

	//initVar(out, outNoUser)

	files, _ := filePathWalkDir("../../../torpedo-data")
	fmt.Println(files)
	count := 0
	for _, filePath := range files {
		if strings.Contains(filePath, "json") {
			mainFunc(filePath, "../../test-data/output/out"+strconv.Itoa(count)+".json", "../../test-data/output/user"+strconv.Itoa(count)+".json")
			count++
		}
	}

	//read test.json file for events

	//convert each event to rudder json by using the mapping

	//[1188111 1192740] -- tt
}

func filePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		/* if !info.IsDir() {
			files = append(files, path)
		} else {
			filesOfDir, _ := filePathWalkDir(path)
			files = append(files, filesOfDir...)
		} */
		files = append(files, path)
		return nil
	})
	return files, err
}

func readDir(root string, files []string) {
	fileInfo, err := ioutil.ReadDir(root)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, file := range fileInfo {
		fmt.Println(file.Name())
		if file.IsDir() {
			readDir(root+"/"+file.Name(), files)
		} else {
			files = append(files, file.Name())
		}

	}
}
