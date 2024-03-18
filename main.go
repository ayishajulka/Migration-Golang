package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	// "regexp"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	 _ "github.com/lib/pq"
	"google.golang.org/grpc"
)

type Currency struct {
	TkCurrencyID    string            `json:"tk_currency_id"`
	Code            string         `json:"code"`
	EnName          string         `json:"en_name"`
	ArName          sql.NullString `json:"ar_name"`
	Symbol          string         `json:"symbol"`
	Precision       int            `json:"precision"`
	TkCountryID     string            `json:"tk_country_id"`
	IsActive        int            `json:"is_active"`
	VersionNo       int            `json:"version_no"`
	InsertUserID    int            `json:"insert_user_id"`
	InsertDttm      time.Time      `json:"insert_dttm"`
	VersionUserID   int            `json:"version_user_id"`
	VersionDttm     time.Time      `json:"version_dttm"`
	ZIndex          int            `json:"zindex"`
	TkCompanyID            string           `json:"tk_company_id"`
	FromCurrencyID         string           `json:"from_currency_id"`
ToCurrencyID                    string `json:"to_currency_id"`

	ValidFrom              time.Time     `json:"valid_from"`
	ValidTo                sql.NullTime  `json:"valid_to"`
	MultiplyRate           float64       `json:"multiply_rate"`
	DivideRate             float64       `json:"divide_rate"`
	BufferRate             float64       `json:"buffer_rate"`
	ConversionId            string         `json:"tk_currency_conversion_id"`
    ConvActive                int          `json:"conv_is_active"`
}

// "faxNo": %s,
	// "mobile": %s,
	// "mobile2": %s,
	// "phone": %s,
	// "phone2": %s,
type DocumentInfo struct {
	IsActive   bool      `json:"isActive"`
	CreatedBy  string    `json:"createdBy"`
	CreatedAt  time.Time `json:"createdAt"`
	ModifiedAt time.Time `json:"modifiedAt"`
	ModifiedBy string    `json:"modifiedBy"`
}

func main() {
	// Connect to PostgreSQL
	pgConnStr := "postgres://icbdev:9RVAp6tDev@192.168.1.170:5432/icbdevdb?sslmode=disable" // Update with your PostgreSQL connection string
	pgDB, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgDB.Close()

	// Connect to Dgraph
	dg := setupDgraphClient()

	// Fetch data from PostgreSQL
	rows, err := pgDB.Query(`
    SELECT
        a.tk_currency_id,
        a.code,
        a.en_name,
        a.ar_name,
        a.symbol,
        a.precision,
        a.tk_country_id,
        a.is_active,
        ac.tk_currency_conversion_id,
        ac.tk_company_id,
        ac.to_currency_id,
        ac.valid_from,
        ac.valid_to,
        ac.multiply_rate,
        ac.divide_rate,
        ac.buffer_rate,
        ac.is_active as conv_is_active
    FROM
        c00082_daca8e17_1d6a_4e4c_98b6_86275a51d7d2.tk_currency AS a
    JOIN
        c00082_daca8e17_1d6a_4e4c_98b6_86275a51d7d2.tk_currency_conversion AS ac
    ON
        a.tk_currency_id = ac.from_currency_id
`)

if err != nil {
    log.Fatalf("Failed to fetch data from PostgreSQL: %v", err)
}

	defer rows.Close()

	// Iterate over rows and add data to Dgraph
	for rows.Next() {
		var currency Currency

		// Scan the row into the Currency struct fields
		if err := rows.Scan(
			&currency.TkCurrencyID,
			&currency.Code,
			&currency.EnName,
			&currency.ArName,
			&currency.Symbol,
			&currency.Precision,
			&currency.TkCountryID,
			&currency.IsActive,
			&currency.ConversionId,
			&currency.TkCompanyID,
			&currency.ToCurrencyID,
			&currency.ValidFrom,
			&currency.ValidTo,
			&currency.MultiplyRate,
			&currency.DivideRate,
			&currency.BufferRate,
			
			&currency.ConvActive,
		); err != nil {
			if err == sql.ErrNoRows {
				log.Println("No rows were returned.")
				return
			}
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Add data to Dgraph
		err := addDataToDgraph(dg, currency)
		if err != nil {
			log.Fatalf("Failed to add data to Dgraph: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error retrieving rows: %v", err)
	}

	fmt.Println("Data migration completed successfully!")
}

func setupDgraphClient() *dgo.Dgraph {
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to Dgraph: %v", err)
	}
	fmt.Println("connected")

	return dgo.NewDgraphClient(api.NewDgraphClient(conn))
}

func addDataToDgraph(dg *dgo.Dgraph, currency Currency) error {
	ctx := context.Background()

	// Convert IsActive and convActive to boolean
	active := currency.IsActive == 1
	cactive := currency.ConvActive == 1

	// Construct JSON mutation for currency
	

	// Construct JSON mutation for currency conversion
	convData := fmt.Sprintf(`{
		"dgraph.type": "CurrencyConversions",
		"uid": "%s",
		"toCurrency": {
			"uid": "%s"
		},
		"company": {
			"uid": "%s"
		},
		"multiplyRate": %f,
		"divideRate": %f,
		"bufferRate": %f,
		"validFrom": "%s",
		"validTo": %s,
		"documentInfo": {
			"isActive": %t,
			"createdBy":"0ddc0874-c477-4def-9b67-c95a42c7f45e",
			"createdAt": "%s",
			"modifiedAt": "%s",
			"modifiedBy":"0ddc0874-c477-4def-9b67-c95a42c7f45e"
		}
	}`, currency.ConversionId, currency.ToCurrencyID, currency.TkCompanyID, currency.MultiplyRate, currency.DivideRate, currency.BufferRate, currency.ValidFrom.Format(time.RFC3339), getNullStringTime(currency.ValidTo), cactive, time.Now(),
	time.Now())
	jsonData := fmt.Sprintf(`{
		"dgraph.type": "Currencies",
		"uid": "%s",
		"code": "%s",
		"enName": "%s",
		"arName": %s,
		"symbol": "%s",
		"precision": %d,
		"country": {
			"uid":"%s"},
		"documentInfo": {
				"isActive": %t,
				"createdBy":"0ddc0874-c477-4def-9b67-c95a42c7f45e",
				"createdAt": "%s",
				"modifiedAt": "%s",
				"modifiedBy":"0ddc0874-c477-4def-9b67-c95a42c7f45e"
			},
			"conversions":%s

	}`, currency.TkCurrencyID, currency.Code, currency.EnName, getNullString(currency.ArName), currency.Symbol, currency.Precision, currency.TkCountryID, active, 
	time.Now(),
	time.Now(),convData)
	fmt.Printf(jsonData)
	// Combine currency and conversion mutations
	mutation := &api.Mutation{
		SetJson:   []byte(jsonData),
		CommitNow: true,
	}

	// Execute mutation
	_, err := dg.NewTxn().Mutate(ctx, mutation)
	if err != nil {
		return err
	}

	return nil
}

// Function to handle null values for time.Time
func getNullStringTime(nt sql.NullTime) string {
	if nt.Valid {
		return fmt.Sprintf(`"%s"`, nt.Time.Format(time.RFC3339))
	}
	return "null"
}


func getNullString(ns sql.NullString) string {
	if ns.Valid {
		return fmt.Sprintf(`"%s"`, ns.String)
	}
	return "null"
}
