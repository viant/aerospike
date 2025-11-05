package main

import (
	"database/sql"
	"fmt"
	"github.com/viant/aerospike"
	"github.com/viant/x"
	"log"
	"reflect"
	"time"
)

func init() {
	_ = aerospike.RegisterSet(x.NewType(reflect.TypeOf(AuthCode{}), x.WithName("auth_code")), aerospike.WithTTLSec(3600))

}

type AuthCode struct {
	Code                string       `aerospike:"code,pk" sqlx:"code,primarykey" json:"code,omitempty"` // the random auth code value
	ClientId            string       `aerospike:"client_id" json:"client_id,omitempty"`                 // OAuth client that requested it
	UserId              string       `aerospike:"user_id" json:"user_id,omitempty"`                     // the authenticated user’s ID (sub)
	RedirectURI         string       `aerospike:"redirect_uri" json:"redirect_uri,omitempty"`           // must match on token exchange
	Scope               string       `aerospike:"scope" json:"scope,omitempty"`
	Resource            string       `aerospike:"resource" json:"resource,omitempty"`
	State               string       `aerospike:"state" json:"state,omitempty"`
	CodeChallenge       string       `aerospike:"code_challenge" json:"code_challenge,omitempty"`              // PKCE code_challenge
	CodeChallengeMethod string       `sqlx:"pkce_method" aerospike:"pkce_method" json:"pkce_method,omitempty"` // “S256” or “plain”
	CreatedAt           time.Time    `aerospike:"created_at" json:"created_at"`                                // when it was issued
	ExpiresAt           time.Time    `aerospike:"expires_at" json:"expires_at"`
	Used                bool         `aerospike:"used" json:"used,omitempty"`                               // flip to true after first redeem
	Has                 *AuthCodeHas `aerospike:"-" setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-"` // set marker for xdatly
}

type AuthCodeHas struct {
	Code                bool
	ClientId            bool
	UserId              bool
	RedirectURI         bool
	Scope               bool
	State               bool
	CodeChallenge       bool
	CodeChallengeMethod bool
	CreatedAt           bool
	ExpiresAt           bool
	Used                bool
}

func main() {

	if err := testIt(); err != nil {
		log.Fatal(err)
	}
}

func testIt() error {
	//10.55.9.247:3000
	db, err := sql.Open("aerospike", "aerospike://10.55.9.247:3000/ns_memory")
	if err != nil {

		return err
	}
	_, err = db.Exec("INSERT INTO auth_code(code) VALUES(?)", "1234")
	if err != nil {
		//return err
	}
	query, err := db.Query("SELECT code FROM auth_code WHERE code = ?", "123")
	if err != nil {
		return err
	}

	fmt.Println(query.Columns())
	if query.Next() {
		id := ""
		query.Scan(&id)
		fmt.Println("READ", id)
		fmt.Println("Sucess!")
	}
	return nil
}
