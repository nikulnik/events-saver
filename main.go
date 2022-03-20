package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func main() {
	cnf := NewConfig()
	conn, err := initClickhouseConnection(cnf)
	if err != nil {
		log.Fatal(err)
	}
	inserter := NewBatchInserter(conn)
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		events, err := readBody(r.Body)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte(err.Error()))
			return
		}
		go insertEvents(events, r.RemoteAddr, inserter)
		w.Write([]byte("OK"))
	})

	log.Fatal(http.ListenAndServe(":80", nil))
}

type Event struct {
	ClientTime chDateTime `json:"client_time"`
	DeviceID   uuid.UUID  `json:"device_id"`
	DeviceOs   string     `json:"device_os"`
	Session    string     `json:"session"`
	Sequence   int32      `json:"sequence"`
	Event      string     `json:"event"`
	ParamInt   int32      `json:"param_int"`
	ParamStr   string     `json:"param_str"`
	IP         net.IP     `json:"ip"`
	ServerTime chDateTime `json:"server_time"`
}

const chTimeLayout = "2006-01-02 15:04:05"

type chDateTime time.Time

func (c *chDateTime) UnmarshalJSON(jsonTime []byte) error {
	t, err := time.Parse(chTimeLayout, string(bytes.Trim(jsonTime, `"`)))
	if err != nil {
		return fmt.Errorf("failed to parse %s to time of %s format due to: %s", jsonTime, chTimeLayout, err)
	}
	*c = chDateTime(t)
	return nil
}

func initClickhouseConnection(cnf Config) (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{cnf.ClickhouseHost + ":" + cnf.ClickhousePort},
			Auth: clickhouse.Auth{
				Database: cnf.ClickhouseDatabase,
				Username: "default",
				Password: "",
			},
			DialTimeout:     time.Second * 7,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)
	if err != nil {
		return nil, err
	}
	log.Println("ping", conn.Ping(ctx))
	log.Printf("stats %#v\n", conn.Stats())
	log.Println(conn.ServerVersion())
	
	err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS events.events
		(
			client_time  DateTime, 
			server_time DateTime,
			ip IPv4,
			device_id UUID,
			device_os String,
			session String,
			sequence Int32,
			event String,
			param_int Int32,
			param_str String
		) 
		ENGINE = MergeTree()
		ORDER BY server_time
	`)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func insertEvents(events [][]byte, remoteAddr string, batchInserter batchInserter) {
	for _, v := range events {
		event := Event{}
		err := json.Unmarshal(v, &event)
		if err != nil {
			log.Println("failed to parse request body element due to:", err)
			return
		}

		// Add IP and server time to the event
		event.ServerTime = chDateTime(time.Now())
		event.IP = getIP(remoteAddr)

		// Send to
		batchInserter.Insert(event)
	}
}

func getIP(addr string) net.IP {
	ip, _, _ := net.SplitHostPort(addr)
	return net.ParseIP(ip)
}

// divides body into []byte parts separated by \n
func readBody(body io.ReadCloser) ([][]byte, error) {
	events := make([][]byte, 0, 30)
	buf := bytes.NewBuffer([]byte{})
	_, err := buf.ReadFrom(body)
	if err != nil {
		return nil, err
	}
	for {
		part, err := buf.ReadBytes(10) // read until \n
		if err == io.EOF {
			if part != nil {
				events = append(events, part)
			}
			return events, nil
		}
		if err != nil {
			return nil, err
		}
		events = append(events, part[:len(part)-1])
	}
}
