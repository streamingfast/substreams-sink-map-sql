package main

import (
	"database/sql"
	"fmt"
	"os"
	test_pb "substreams-sink-map-sql/pb"
	sql2 "substreams-sink-map-sql/sql"
	"time"

	"github.com/jhump/protoreflect/desc/protoparse"
	_ "github.com/lib/pq"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	"google.golang.org/protobuf/proto"
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
	protoFile := "/Users/cbillett/devel/sf/substreams-sink-map-sql/proto/test.proto"

	// Create a new parser
	parser := protoparse.Parser{}

	// Parse the .proto file to get descriptors
	fds, err := parser.ParseFiles(protoFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse .proto file: %v", err))
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
		panic(fmt.Sprintf("Failed to connect to database: %v", err))
	}

	schema, err := sql2.NewSchema("foo", 1, "test.Transactions", fileDesc, logger)
	database, err := sql2.NewDatabase(schema, psqlDB, "test.Transactions", fileDesc, logger)

	if err != nil {
		panic(fmt.Errorf("failed to create database: %w", err))
	}

	blankCursor, err := sink.NewCursor("")
	if err != nil {
		panic(fmt.Errorf("failed to create cursor: %w", err))
	}

	output := &test_pb.Transactions{
		Foo: "toto",
		Transactions: []*test_pb.Transaction{
			{
				TrxHash: "tx.hash.1",
				Entities: []*test_pb.Entity{
					{
						Item: &test_pb.Entity_Payment{
							&test_pb.Payment{
								Mint: &test_pb.Mint{
									Timestamp: 0,
									To:        "to.hash.1",
									Amount:    10,
								},
								Type: test_pb.PaymentType_FLEET_MANAGER,
							},
						},
					},
				},
			}, {
				TrxHash: "tx.hash.2",
				Entities: []*test_pb.Entity{
					{
						Item: &test_pb.Entity_Transfers{
							&test_pb.Transfer{
								Timestamp: 0,
								From:      "from.hash.1",
								To:        "to.hash.1",
								Amount:    99,
							},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(output)
	err = database.ProcessEntity(data, 1, "block.hash.1", time.Now(), blankCursor)
	if err != nil {
		panic(fmt.Errorf("failed to process entity: %w", err))
	}
}
