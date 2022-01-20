/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package conn

import (
	"fmt"
	"reflect"
	"testing"

	reff "github.com/modern-go/reflect2"
)

type User struct {
	Name    string
	Surname string
	Age     int
}

func TestQuery_Scan(t *testing.T) {
	//var arr []User
	//fmt.Println(reflect.TypeOf(arr))
	//test(&arr)
	//fmt.Println(reflect.TypeOf(arr))
	//fmt.Println(len(arr))
	//fmt.Println(cap(arr))

	var user = User{
		Name:    "test123123",
		Surname: "test",
		Age:     10,
	}
	check(&user.Name)
	fmt.Println(user)
}

func test(arr interface{}) {
	s := reflect.Indirect(reflect.ValueOf(arr))
	fmt.Println(reflect.TypeOf(s))
	fmt.Println(s.Len())
	a := reff.TypeOf(arr)
	fmt.Println(a)
	s.Set(reflect.AppendSlice(s, reflect.MakeSlice(reflect.TypeOf(arr).Elem(), 10, 10)))
	fmt.Println(s.Len())
}

func check(ptr *string) {
	if ptr != nil {
		test := "test"
		ptr = &test
	}

}
