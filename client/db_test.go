package client_test

import (
	"fmt"
	"log"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/server"
)

func setup() (*server.TestServer, *client.DB) {
	s := server.StartTestServer(nil)
	db := client.Open("https://root@" + s.ServingAddr())
	return s, db
}

func ExampleDBPutGet() {
	s, db := setup()
	defer s.Stop()

	if err := db.Put("aa", "1"); err != nil {
		log.Fatal(err)
	}
	result := db.Get("aa")
	fmt.Println(result.Rows[0].String())

	// Output:
	// aa:1
}

func ExampleDBInc() {
	s, db := setup()
	defer s.Stop()

	if r := db.Inc("aa", 100); r.Err != nil {
		log.Fatal(r.Err)
	}
	result := db.Get("aa")
	fmt.Println(result.Rows[0].String())

	// Output:
	// aa:100
}

func ExampleDBBatch() {
	s, db := setup()
	defer s.Stop()

	b := client.B.Get("aa").Put("bb", "2")
	if err := db.Run(b); err != nil {
		log.Fatal(err)
	}
	fmt.Println(b.Results[0].Rows[0].String())
	fmt.Println(b.Results[1].Rows[0].String())

	// Output:
	// aa:nil
	// bb:2
}

func ExampleDBScan() {
	s, db := setup()
	defer s.Stop()

	b := client.B.Put("aa", "1").Put("ab", "2").Put("bb", "3")
	if err := db.Run(b); err != nil {
		log.Fatal(err)
	}
	r := db.Scan("a", "b", 100)
	if r.Err != nil {
		log.Fatal(r.Err)
	}
	fmt.Println(len(r.Rows))
	fmt.Println(r.Rows[0].String())
	fmt.Println(r.Rows[1].String())

	// Output:
	// 2
	// aa:1
	// ab:2
}

func ExampleDBDel() {
	s, db := setup()
	defer s.Stop()

	b := client.B.Put("aa", "1").Put("ab", "2").Put("ac", "3")
	if err := db.Run(b); err != nil {
		log.Fatal(err)
	}
	if err := db.Del("ab"); err != nil {
		log.Fatal(err)
	}
	r := db.Scan("a", "b", 100)
	if r.Err != nil {
		log.Fatal(r.Err)
	}
	fmt.Println(len(r.Rows))
	fmt.Println(r.Rows[0].String())
	fmt.Println(r.Rows[1].String())

	// Output:
	// 2
	// aa:1
	// ac:3
}

func ExampleDBTx() {
	s, db := setup()
	defer s.Stop()

	err := db.Tx(func(tx *client.Tx) error {
		return tx.Commit(client.B.Put("aa", "1").Put("ab", "2"))
	})
	if err != nil {
		log.Fatal(err)
	}

	r := db.Get("aa", "ab")
	if r.Err != nil {
		log.Fatal(r.Err)
	}

	fmt.Println(r.Rows[0].String())
	fmt.Println(r.Rows[1].String())

	// Output:
	// aa:1
	// ab:2
}
