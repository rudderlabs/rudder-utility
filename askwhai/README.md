### Purpose

- This script will take exported data from Amplitude and push them to RudderStack data-plane in the format accepted by RudderStack.

### How to run

- Download the repo and change directory to the repo path
- This script is written in Go, set up your Go environment before running the script.
  https://golang.org/doc/install
  https://blog.golang.org/using-go-modules
- All library dependencies for the script are in vendor folder.
- To get the list of command line flags run :
  go run -mod=vendor convert.go -h
- To run the script run the below command:
  go run -mod=vendor convert.go -filePath= "AM export data file, default is .data/input.json"
  -dataPlaneURI="RudderStack data plane URL" -writeKey="source write key"
