package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client"
	"log"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	writethread = 5    //写线程数
	makethread  = 5    //制造数据的线程数
	sampleSize  = 5000 //一批数据的大小
	sizeofchan  = 100  //数据通道大小
	sizeofmem   = 10   //表数量

	createNum uint64 = 0
	sendNum   uint64 = 0
	dataNum   uint64 = 0
)

type config struct {
	Wt  int `toml:"writethread"`
	Mt  int `toml:"makethread"`
	Ss  int `toml:"samplesize"`
	Soc int `toml:"sizeofchan"`
	Som int `toml:"sizeofmem"`
}

func main() {

	decodefile()
	var wg sync.WaitGroup
	wg.Add(1)
	go TestExampleClient_Write()
	go printStat()
	wg.Wait()
}

func decodefile() {
	var conf config
	filepath := "./cmd/tools/write2host/write2host.conf"

	if _, err := toml.DecodeFile(filepath, &conf); err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(conf)
	writethread = conf.Wt
	makethread = conf.Mt
	sampleSize = conf.Ss
	sizeofchan = conf.Soc
	sizeofmem = conf.Som

}
func printStat() {
	begin := time.Now()
	for true {
		timer := time.NewTicker(1 * time.Second)
		select {
		case <-timer.C:
			{
				fmt.Println(fmt.Sprintf("craeateNum is %d,sendNum is %d", createNum, sendNum))
				//count += 5000 * 5
				elapse := time.Since(begin)
				speed := int(dataNum) / (int(elapse) / 1000000)
				fmt.Println(speed, "/ms")
			}
		}
	}
}
func TestExampleClient_Write() {
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "172.16.72.38", 8086))
	if err != nil {
		log.Fatal(err)
	}
	conns := make([]*client.Client, 0)
	for i := 0; i < writethread; i++ {
		conn, err := client.NewClient(client.Config{URL: *host})
		if err != nil {
			log.Fatal(err)
			return
		}
		conns = append(conns, conn)
	}
	datachan := make(chan client.BatchPoints, sizeofchan)

	for i := 0; i < makethread; i++ {
		go makedata2(datachan)
	}

	//batchsize := sampleSize*writethread
	var wg sync.WaitGroup
	for true {
		for i, _ := range conns {
			wg.Add(1)
			go func(index int) {
				_, err := conns[i].Write(<-datachan)
				if err != nil {
					log.Fatal(err)
				}
				atomic.AddUint64(&sendNum, 1)
				atomic.AddUint64(&dataNum, uint64(sampleSize))
				wg.Done()
			}(i)
		}
		wg.Wait()
		//atomic.AddUint64(&dataNum,uint64(batchsize))
	}
}

func makedata1(points chan client.BatchPoints) {
	var (
		shapes = []string{"circle", "rectangle", "square", "triangle"}
		colors = []string{"red", "blue", "green"}
	)
	rand.Seed(42)
	for true {
		pts := make([]client.Point, sampleSize)
		for i := 0; i < sampleSize; i++ {
			pts[i] = client.Point{
				Measurement: fmt.Sprintf("mem%s", strconv.Itoa(rand.Int()%sizeofmem)),
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
		}
		points <- client.BatchPoints{
			Points:   pts,
			Database: "mydb",
		}
		atomic.AddUint64(&createNum, 1)
	}
}
func makedata2(points chan client.BatchPoints) {
	var (
		shapes = []string{"circle", "rectangle", "square", "triangle"}
		colors = []string{"red", "blue", "green"}
	)
	//for true {
	ts := time.Now()
	for index := 0; index <= 10000; index++ {
		pts := make([]client.Point, sampleSize)
		//index := rand.Int() % sizeofmem
		for i := 0; i < sampleSize; i++ {
			//ts = ts+1
			ts = ts.Add(1 * time.Millisecond)
			pts[i] = client.Point{
				//Measurement: fmt.Sprintf("testcantselect%s", strconv.Itoa(index)),
				Measurement: "testmultshard",
				Tags: map[string]string{
					"color": shapes[rand.Int()%len(colors)],
					"shape": shapes[rand.Int()%len(shapes)],
				},
				Fields: map[string]interface{}{
					"value": rand.Intn(sampleSize),
				},
				Time:      ts,
				Precision: "ns",
			}
		}
		points <- client.BatchPoints{
			Points:   pts,
			Database: "mydb",
		}
		atomic.AddUint64(&createNum, 1)
	}
	fmt.Println("make ok!!!!!!!!!!!!")
}
