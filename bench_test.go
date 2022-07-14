/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package pap

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"testing"
	"time"

	"github.com/georgysavva/scany/pgxscan"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/lib/pq"
)

type Goods struct {
	ID          int
	BrandID     int
	Title       string
	Description string
	Excerpt     string
}

func BenchmarkMinimalUnpreparedSelectWithoutStatementCache(b *testing.B) {
	p, _ := Start(os.Getenv("PAP_TEST_DATABASE"))

	str := "select id, brand_id, title, description, excerpt from goods where brand_id = $1 order by title limit 20"
	fmt.Println("Start test")
	b.ResetTimer()
	b.ReportAllocs()
	//b.SetParallelism(10)
	i := 10
	b.RunParallel(func(pb *testing.PB) {
		for {
			i++
			runQuery(p, str, b, i)
		}
	})

}

func runQuery(p *Pap, str string, b *testing.B, i int) {
	var arr []Goods
	async := p.QueryAsync(str, i)
	err := async(&arr)
	if err != nil {
		b.Fatal(err)
	}

	if len(arr) > 0 && arr[0].BrandID != i {
		b.Fatalf("expected %d, got %d", i, arr[0].BrandID)
	}
	arr = arr[:0]
}

func BenchmarkMinimalUnpreparedSelectWithoutStatementCachePGX(b *testing.B) {
	ctx := context.Background()
	db, _ := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))

	str := "select id, title, description, excerpt from goods where id < $1 order by title limit 20"

	var arr []Goods
	var arr1 []Goods
	var arr2 []Goods
	var arr3 []Goods
	var arr4 []Goods
	var arr5 []Goods
	var arr6 []Goods
	var arr7 []Goods
	var arr8 []Goods
	var arr9 []Goods
	b.ResetTimer()
	b.ReportAllocs()

	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr1, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr1 = arr1[:0]
		}
	}()
	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr2, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr2 = arr2[:0]
		}
	}()
	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr3, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr3 = arr3[:0]
		}
	}()
	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr4 = arr4[:0]
		}
	}()
	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr5, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr5 = arr5[:0]
		}
	}()
	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr6, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr6 = arr6[:0]
		}
	}()

	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr7, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr7 = arr7[:0]
		}
	}()

	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr8, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr8 = arr8[:0]
		}
	}()

	go func() {
		for i := 10; i < 100000; i++ {
			err := pgxscan.Select(
				ctx, db, &arr9, str, i,
			)
			if err != nil {
				b.Fatal(err)
			}

			//if arr[0].ID != i {
			//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
			//}
			arr9 = arr9[:0]
		}
	}()

	for i := 10; i < 100000; i++ {
		err := pgxscan.Select(
			ctx, db, &arr, str, i,
		)
		if err != nil {
			b.Fatal(err)
		}

		//if arr[0].ID != i {
		//	b.Fatalf("expected %d, got %d", i, arr[0].ID)
		//}
		arr = arr[:0]
	}
}

func BenchmarkGCEnabled(b *testing.B) {
	debug.SetGCPercent(100)
	time.Sleep(time.Second)
	b.ResetTimer()
	b.ReportAllocs()

	var a []byte
	for i := 0; i < b.N; i++ {
		a = make([]byte, 10000)
	}

	_ = a
}

func BenchmarkGCDisabled(b *testing.B) {
	debug.SetGCPercent(-1)
	time.Sleep(time.Second)
	b.ResetTimer()
	b.ReportAllocs()

	var a []byte
	for i := 0; i < b.N; i++ {
		a = make([]byte, 10000)
	}

	_ = a
}
