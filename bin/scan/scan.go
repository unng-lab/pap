/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		log.Fatal(err)
	}

	str := "select id, brand_id from goods where brand_id = $1 order by title limit 20"

	rows, err := db.Query(str, 100000)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var id, brandID string
	// Loop through the first result set.
	for rows.Next() {
		err := rows.Scan(&id, &brandID)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(id, brandID)
	}
}
