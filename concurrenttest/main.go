package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5439
	user     = "username"
	password = "password"
	dbname   = "postgis"
)

// docker run -e POSTGRES_DB=stac -e POSTGRES_PASSWORD=admin -e POSTGRES_USER=admin -e PGUSER=admin -e PGPASSWORD=admin -e PGDATABASE=stac -p 5432:5432 ghcr.io/stac-utils/pgstac:v0.8.2 postgres -c 'log_statement=all'

func main() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Successfully connected!")

	query := `SELECT search('{"limit": 1}'::jsonb);`
	log.Printf("Search query: %#v", query)

	wg := sync.WaitGroup{}

	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(threadID int) {
			row := db.QueryRow(query)
			err = row.Err()
			if err != nil {
				log.Fatalf("[%d] Error: %v", threadID, err)
			}

			var item json.RawMessage
			err = row.Scan(&item)
			if err != nil {
				log.Fatalf("[%d] Error: %v", threadID, err)
			}

			log.Printf("[%d] Result: %s", threadID, string(item))

			wg.Done()
		}(i)
	}
	wg.Wait()
}
