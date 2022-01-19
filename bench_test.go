package pap

import (
	"os"
	"testing"
)

type goods struct {
	ID          int
	Description string
}

func BenchmarkMinimalUnpreparedSelectWithoutStatementCache(b *testing.B) {
	p, _ := Start(os.Getenv("PAP_TEST_DATABASE"))

	str := "select id, description from goods limit 10"
	var id int
	var desc string
	b.ResetTimer()
	b.ReportAllocs()

	for i := 10; i < b.N; i++ {
		async := p.QueryAsync(str, i)
		err := async(&id, &desc)
		if err != nil {
			b.Fatal(err)
		}

		if id != i {
			b.Fatalf("expected %d, got %d", i, id)
		}

	}
}
