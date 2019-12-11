package client_test

import (
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client"
	"sync"
	"testing"
)

func ExampleNewClient() {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "172.16.41.10", 8086))
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: this assumes you've setup a user and have setup shell env variables,
	// namely INFLUX_USER/INFLUX_PWD. If not just omit Username/Password below.
	conf := client.Config{
		URL:      *host,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PWD"),
	}
	con, err := client.NewClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connection", con)

}
func TestWrite(t *testing.T) {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "172.16.72.38", 8086))
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: this assumes you've setup a user and have setup shell env variables,
	// namely INFLUX_USER/INFLUX_PWD. If not just omit Username/Password below.
	conf := client.Config{
		URL:      *host,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PWD"),
	}
	con, err := client.NewClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connection", con)
}

func ExampleClient_Ping() {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "localhost", 8086))
	if err != nil {
		log.Fatal(err)
	}
	con, err := client.NewClient(client.Config{URL: *host})
	if err != nil {
		log.Fatal(err)
	}

	dur, ver, err := con.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Happy as a hippo! %v, %s", dur, ver)
}

func ExampleClient_Query() {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "localhost", 8086))
	if err != nil {
		log.Fatal(err)
	}
	con, err := client.NewClient(client.Config{URL: *host})
	if err != nil {
		log.Fatal(err)
	}

	q := client.Query{
		Command:  "select count(value) from shapes",
		Database: "square_holes",
	}
	if response, err := con.Query(q); err == nil && response.Error() == nil {
		log.Println(response.Results)
	}
}

func TestExampleClient_Write(t *testing.T) {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "172.16.41.10", 8086))
	if err != nil {
		log.Fatal(err)
	}
	con1, err := client.NewClient(client.Config{URL: *host})
	con2, err := client.NewClient(client.Config{URL: *host})
	con3, err := client.NewClient(client.Config{URL: *host})
	con4, err := client.NewClient(client.Config{URL: *host})
	con5, err := client.NewClient(client.Config{URL: *host})
	if err != nil {
		log.Fatal(err)
	}

	datachan := make(chan client.BatchPoints, 1000)

	for i := 0; i < 5; i++ {
		go makedata1(datachan)
	}

	count := 0
	begin := time.Now()
	var wg sync.WaitGroup
	for true {
		wg.Add(5)
		//bps := <- datachan
		go func() {
			_, err = con1.Write(<-datachan)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}()
		go func() {
			_, err = con2.Write(<-datachan)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}()
		go func() {
			_, err = con3.Write(<-datachan)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}()
		go func() {
			_, err = con4.Write(<-datachan)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}()
		go func() {
			_, err = con5.Write(<-datachan)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}()
		wg.Wait()
		count += 5000 * 5
		elapse := time.Since(begin)
		speed := count / (int(elapse) / 1000000)
		fmt.Println(speed, "/ms")
	}
}

func makedata1(points chan client.BatchPoints) {
	var (
		shapes     = []string{"circle", "rectangle", "square", "triangle"}
		colors     = []string{"red", "blue", "green"}
		sampleSize = 5000
		pts        = make([]client.Point, sampleSize)
	)
	rand.Seed(42)
	for i := 0; i < sampleSize; i++ {
		pts[i] = client.Point{
			Measurement: fmt.Sprintf("mem%s", strconv.Itoa(rand.Int()%5)),
			Tags: map[string]string{
				"color": strconv.Itoa(rand.Intn(len(colors))),
				"shape": strconv.Itoa(rand.Intn(len(shapes))),
			},
			Fields: map[string]interface{}{
				"value": rand.Intn(sampleSize),
			},
			Time:      time.Now(),
			Precision: "ns",
		}
		points <- client.BatchPoints{
			Points:   pts,
			Database: "mydb2",
		}
	}
}

func makedata2(points chan client.BatchPoints) {
	var (
		shapes     = []string{"circle", "rectangle", "square", "triangle"}
		colors     = []string{"red", "blue", "green"}
		sampleSize = 5000
		pts        = make([]client.Point, sampleSize)
	)
	rand.Seed(42)
	for i := 0; i < sampleSize; i++ {
		pts[i] = client.Point{
			Measurement: "shapes",
			Tags: map[string]string{
				"color": strconv.Itoa(rand.Intn(len(colors))),
				"shape": strconv.Itoa(rand.Intn(len(shapes))),
			},
			Fields: map[string]interface{}{
				"value": rand.Intn(sampleSize),
			},
			Time:      time.Now(),
			Precision: "ns",
		}
		points <- client.BatchPoints{
			Points:   pts,
			Database: "mydb",
		}
	}
}
