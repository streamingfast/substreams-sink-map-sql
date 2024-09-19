package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	sql2 "substreams-sink-map-sql/sql"

	"github.com/jhump/protoreflect/desc/protoparse"
	_ "github.com/lib/pq"
	"github.com/streamingfast/logging"
)

type PsqlInfo struct {
	Host     string
	Port     int
	User     string
	Password string
	Dbname   string
}

func (i *PsqlInfo) GetPsqlInfo() string {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		i.Host, i.Port, i.User, i.Password, i.Dbname,
	)
	return psqlInfo
}

func main() {
	logger, _ := logging.ApplicationLogger("honey-tracker", "honey-tracker")

	// Path to your .proto file
	protoFile := "/Users/cbillett/devel/sf/substreams-sink-map-sql/cmd/test.proto"

	// Create a new parser
	parser := protoparse.Parser{}

	// Parse the .proto file to get descriptors
	fds, err := parser.ParseFiles(protoFile)
	if err != nil {
		log.Fatalf("Failed to parse .proto file: %v", err)
	}

	// fds is a []*desc.FileDescriptor, we take the first one for simplicity
	fileDesc := fds[0]

	// Print the name of the file
	fmt.Printf("Parsed FileDescriptor: %s\n", fileDesc.GetName())

	psqlInfo := &PsqlInfo{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: os.Getenv("POSTGRES_DB_PWD"),
		Dbname:   "postgres",
	}

	psqlDB, err := sql.Open("postgres", psqlInfo.GetPsqlInfo())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	_, err = sql2.NewDatabase(&sql2.Schema{
		Name:    "foo",
		Version: 1,
	}, psqlDB, fileDesc, logger)

	if err != nil {
		panic(fmt.Errorf("failed to create database: %w", err))
	}

}
