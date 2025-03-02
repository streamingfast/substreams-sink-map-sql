package sql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jmoiron/sqlx"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	hm "github.com/streamingfast/substreams-sink-map-sql/pb/test/hm"
	"github.com/test-go/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestDatabase_ProcessEntity(t *testing.T) {
	logger, _ := logging.ApplicationLogger("honey-tracker", "honey-tracker")

	// Path to your .proto file
	protoFile := "test/hm/hm.proto"

	// Create a new parser
	parser := protoparse.Parser{}
	parser.ImportPaths = []string{"/Users/cbillett/devel/sf/substreams-sink-map-sql/proto"}

	// Parse the .proto file to get descriptors
	fds, err := parser.ParseFiles(protoFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse .proto file: %v", err))
	}

	// fds is a []*desc.FileDescriptor, we take the first one for simplicity
	fileDesc := fds[0]

	// Print the name of the file
	fmt.Printf("Parsed FileDescriptor: %s\n", fileDesc.GetName())

	schema, err := NewSchema("foo", "test.hm.ModuleOutput", fileDesc, logger)
	require.NoError(t, err)

	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Database("hm").
			Username("user").
			Password("pwd"),
	)
	err = pg.Start()
	require.NoError(t, err)
	defer pg.Stop()

	db, err := sql.Open("postgres", "dbname=hm user=user password=pwd sslmode=disable")
	require.NoError(t, err)
	dbx, err := sqlx.Open("postgres", "dbname=hm user=user password=pwd sslmode=disable")
	require.NoError(t, err)

	database, err := NewDatabase(schema, db, "test.hm.ModuleOutput", fileDesc, logger)
	require.NoError(t, err)

	blankCursor, err := sink.NewCursor("")
	if err != nil {
		panic(fmt.Errorf("failed to create cursor: %w", err))
	}

	output := &hm.ModuleOutput{
		Transactions: []*hm.Transaction{
			{
				TrxHash: "tx.hash.1",
				Entities: []*hm.Entity{
					{
						Item: &hm.Entity_Payment{
							&hm.Payment{
								Mint: &hm.Mint{
									Timestamp: 0,
									To:        "to.hash.1",
									Amount:    10,
								},
								Type: hm.PaymentType_FLEET_MANAGER,
							},
						},
					},
				},
			}, {
				TrxHash: "tx.hash.2",
				Entities: []*hm.Entity{
					{
						Item: &hm.Entity_Transfers{
							&hm.Transfer{
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

	fmt.Println("Printing content of tables...")

	// Define structures for transactions, entity, transfer
	type Transaction struct {
		ID          int       `db:"id"`
		BlockNumber int       `db:"block_number"`
		TrxHash     string    `db:"trx_hash"`
		CreatedAt   time.Time `db:"created_at"`
	}

	type Entity struct {
		ID            int    `db:"id"`
		BlockNumber   int    `db:"block_number"`
		TransactionID int    `db:"transaction_id"`
		ItemType      string `db:"item_type"`
		PaymentId     *int   `db:"payment_id"`

		MapCreateID *int `db:"map_create_id"`
		TransferID  *int `db:"transfers_id"`
		MintID      *int `db:"mints_id"`
		BurnID      *int `db:"burns_id"`
		InitAccount *int `db:"initialized_account_id"`
	}

	type Transfer struct {
		ID          int     `db:"id"`
		BlockNumber int     `db:"block_number"`
		EntityID    int     `db:"entity_id"`
		From        string  `db:"from"`
		To          string  `db:"to"`
		Amount      float64 `db:"amount"`
		Timestamp   int64   `db:"timestamp"`
	}

	// Print transactions table
	var transactions []Transaction
	err = sqlx.Select(dbx, &transactions, "SELECT * FROM foo.transactions")
	require.NoError(t, err)
	fmt.Println("Transactions Table:")
	for _, trx := range transactions {
		fmt.Printf("%+v\n", trx)
	}

	// Print entity table
	var entities []Entity
	err = sqlx.Select(dbx, &entities, "SELECT * FROM foo.entities")
	require.NoError(t, err)
	fmt.Println("Entity Table:")
	for _, ent := range entities {
		fmt.Printf("%+v\n", ent)
	}

	// Print transfer table
	var transfers []Transfer
	err = sqlx.Select(dbx, &transfers, "SELECT * FROM foo.transfer")
	require.NoError(t, err)
	fmt.Println("Transfer Table:")
	for _, trf := range transfers {
		fmt.Printf("%+v\n", trf)
	}

	// Print payment table
	type Payment struct {
		ID          int    `db:"id"`
		BlockNumber int    `db:"block_number"`
		Type        string `db:"type"`
		MintID      int    `db:"mint_id"`
	}

	var payments []Payment
	err = sqlx.Select(dbx, &payments, "SELECT * FROM foo.payment")
	require.NoError(t, err)
	fmt.Println("Payment Table:")
	for _, pmt := range payments {
		fmt.Printf("%+v\n", pmt)
	}

}
