/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package pap

import (
	"context"
	"os"
	"testing"

	"github.com/georgysavva/scany/pgxscan"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Goods struct {
	ID          int
	Description string
}

func BenchmarkMinimalUnpreparedSelectWithoutStatementCache(b *testing.B) {
	p, _ := Start(os.Getenv("PAP_TEST_DATABASE"))

	str := "select id, description from goods where id >= $1 order by id limit 20"
	var arr []Goods
	b.ResetTimer()
	b.ReportAllocs()

	for i := 10; i < 10000; i++ {
		async := p.QueryAsync(str, i)
		err := async(&arr)
		if err != nil {
			b.Fatal(err)
		}

		//if arr[0].ID < i {
		//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
		//}
	}
}

func BenchmarkMinimalUnpreparedSelectWithoutStatementCachePGX(b *testing.B) {
	ctx := context.Background()
	db, _ := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))

	str := "select id, description from goods where id >= $1 order by id asc limit 20"

	var arr []Goods
	b.ResetTimer()
	b.ReportAllocs()

	for i := 10; i < 10000; i++ {
		err := pgxscan.Select(
			ctx, db, &arr, str, i,
		)
		if err != nil {
			b.Fatal(err)
		}

		//if arr[0].ID != i {
		//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
		//}
	}
}
