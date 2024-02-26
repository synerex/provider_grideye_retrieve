package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	grideye "github.com/synerex/proto_grideye"
	api "github.com/synerex/synerex_api"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

// datastore provider provides Datastore Service.

// DataStore :
type DataStore interface {
	store(str string)
}

var (
	nodesrv   = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local     = flag.String("local", "", "Local Synerex Server")
	sendfile  = flag.String("sendfile", "", "Sending file name") // only one file
	startDate = flag.String("startDate", "02-07", "Specify Start Date")
	endDate   = flag.String("endDate", "12-31", "Specify End Date")
	startTime = flag.String("startTime", "00:00", "Specify Start Time")
	endTime   = flag.String("endTime", "24:00", "Specify End Time")
	dir       = flag.String("dir", "", "Directory of data storage") // for all file
	all       = flag.Bool("all", false, "Send all file in Dir")     // for all file
	speed     = flag.Float64("speed", 1.0, "Speed of sending packets")
	multi     = flag.Int("multi", 1, "Specify sending multiply messages")
	mu        sync.Mutex
	version   = "0.01"
	baseDir   = "store"
	dataDir   string
	ds        DataStore
)

func init() {
	var err error
	dataDir, err = os.Getwd()
	if err != nil {
		fmt.Printf("Can't obtain current wd")
	}
	dataDir = filepath.ToSlash(dataDir) + "/" + baseDir
	ds = &FileSystemDataStore{
		storeDir: dataDir,
	}
}

// FileSystemDataStore :
type FileSystemDataStore struct {
	storeDir  string
	storeFile *os.File
	todayStr  string
}

// #### <Retrieve というより Store 用のコード群> ####

// open file with today info
func (fs *FileSystemDataStore) store(str string) {
	const layout = "2006-01-02"
	day := time.Now()
	todayStr := day.Format(layout) + ".csv"
	if fs.todayStr != "" && fs.todayStr != todayStr {
		fs.storeFile.Close()
		fs.storeFile = nil
	}
	if fs.storeFile == nil {
		_, er := os.Stat(fs.storeDir)
		if er != nil { // create dir
			er = os.MkdirAll(fs.storeDir, 0777)
			if er != nil {
				fmt.Printf("Can't make dir '%s'.", fs.storeDir)
				return
			}
		}
		fs.todayStr = todayStr
		file, err := os.OpenFile(filepath.FromSlash(fs.storeDir+"/"+todayStr), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Can't open file '%s'", todayStr)
			return
		}
		fs.storeFile = file
	}
	fs.storeFile.WriteString(str + "\n")
}

func supplyGridEyeCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	ge := &grideye.GridEye{}

	err := proto.Unmarshal(sp.Cdata.Entity, ge)
	if err == nil { // get GridEye Data
		ts0 := ptypes.TimestampString(ge.Ts)
		ld := fmt.Sprintf("%s, %s, %s, %s", ts0, ge.Hostname, ge.Mac, ge.Ip)
		ds.store(ld)
		for _, ev := range ge.Data {
			ts := ptypes.TimestampString(ev.Ts)
			line := fmt.Sprintf("%s, %s, %d, %s, %s, ", ts, ge.DeviceId, ev.Seq, ev.Typ, ev.Id)
			switch ev.Typ {
			case "sensor":
				line = line + fmt.Sprintf("%v, %v", ev.Ts, ev.Temps)
			case "keepalive":
				line = line + fmt.Sprintf("%v", ev.Ts)
			}
			ds.store(line)
		}
	}
}

func subscribeGridEyeSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyGridEyeCallback)
	log.Fatal("Error on subscribe")
}

// #### </Retrieve というより Store 用のコード群> ####

const dateFmt = "2006-01-02T15:04:05.999Z"

func atoUint(s string) uint32 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint32(r)
}

func atoUint64(s string) uint64 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint64(r)
}

func atoFloat32(s string) float32 {
	r, err := strconv.ParseFloat(s, 32)
	if err != nil {
		log.Print("err", err)
	}
	return float32(r)
}

func atoFloat64(s string) float64 {
	r, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Print("err", err)
	}
	return float64(r)
}

func getHourMin(dt string) (hour int, min int) {
	st := strings.Split(dt, ":")
	hour, _ = strconv.Atoi(st[0])
	min, _ = strconv.Atoi(st[1])
	return hour, min
}

func getMonthDate(dt string) (month int, date int) {
	st := strings.Split(dt, "-")
	month, _ = strconv.Atoi(st[0])
	date, _ = strconv.Atoi(st[1])
	return month, date
}

// sending GridEye Data File.
func sendingGridEyeFile(client *sxutil.SXServiceClient) {
	// file
	fp, err := os.Open(*sendfile)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp) // csv reader

	last := time.Now()
	var ge *grideye.GridEye = nil
	evts := make([]*grideye.GridEyeEvent, 0, 1)
	ges := make([]*grideye.GridEye, 0, 1)
	mcount := 0      // count multiple packets
	started := false // start flag
	stHour, stMin := getHourMin(*startTime)
	edHour, edMin := getHourMin(*endTime)

	for scanner.Scan() { // read one line.
		dt := scanner.Text()
		token := strings.Split(dt, ",")
		//		fmt.Println("Tokens:", token[0], token[1], token[2], token[3], token[4], token[5], token[6], token[10], token[11])

		switch token[2] {
		case "alive":
		case "counter":
		default:
			tm, _ := time.Parse(dateFmt, token[0]) // RFC3339Nano
			// check timestamp of data
			if !started {
				if (tm.Hour() > stHour || (tm.Hour() == stHour && tm.Minute() >= stMin)) &&
					(tm.Hour() < edHour || (tm.Hour() == edHour && tm.Minute() <= edMin)) {
					started = true
					log.Printf("Start output! %v", tm)
				} else {
					continue // skip all data
				}
			} else {
				if tm.Hour() > edHour || (tm.Hour() == edHour && tm.Minute() > edMin) {
					started = false
					log.Printf("Stop  output! %v", tm)
					continue
				}
			}

			tp, _ := ptypes.TimestampProto(tm)
			temps := make([]float64, 64)
			tempToken := strings.Split(token[11][1:len(token[11])-1], " ")
			for j := 0; j < 64; j++ {
				temps[j] = atoFloat64(tempToken[j])
			}
			evt := &grideye.GridEyeEvent{
				Typ:   token[8],
				Ts:    tp,
				Seq:   atoUint64(token[10]),
				Id:    token[9],
				Temps: temps,
			}
			evts = append(evts, evt)
			sq, _ := strconv.Atoi(token[6])

			ge = &grideye.GridEye{
				Ts:       tp,
				DeviceId: token[1],
				Hostname: token[2],
				Location: token[3],
				Mac:      token[4],
				Ip:       token[5],
				Seq:      uint64(sq),
				Data:     evts,
			}
			//			log.Printf("%s:%s", token[3], dt)

			//			log.Print("Grid::::")
			if len(evts) > 0 {
				if *multi == 1 { // sending each packets
					out, _ := proto.Marshal(ge)
					//					log.Printf("grid_final: len %d", len(out))
					cont := pb.Content{Entity: out}
					smo := sxutil.SupplyOpts{
						Name:  "GridEye",
						Cdata: &cont,
					}
					_, nerr := client.NotifySupply(&smo)
					if nerr != nil {
						log.Printf("Send Fail! %v\n", nerr)
					} else {
						//						log.Printf("Sent OK! %#v\n", ge)
					}
					if *speed < 0 { // sleep for each packet
						time.Sleep(time.Duration(-*speed) * time.Millisecond)
					}

				} else { // sending multiple packets
					mcount++
					ge.Data = evts
					ges = append(ges, ge)
					if mcount > *multi { // now sending!
						gess := &grideye.GridEyes{
							Messages: ges,
						}
						out, _ := proto.Marshal(gess)
						cont := pb.Content{Entity: out}
						smo := sxutil.SupplyOpts{
							Name:  "GridEyeMulti",
							Cdata: &cont,
						}
						_, nerr := client.NotifySupply(&smo)
						if nerr != nil {
							log.Printf("Send Fail! %v\n", nerr)
						} else {
							log.Printf("Sent OK! %d bytes: %s\n", len(out), ptypes.TimestampString(ge.Ts))
						}
						if *speed < 0 {
							time.Sleep(time.Duration(-*speed) * time.Millisecond)
						}
						ges = make([]*grideye.GridEye, 0, 1)
						mcount = 0
					}
				}
			}

			evts = make([]*grideye.GridEyeEvent, 0, 1)
			tm, er := time.Parse(dateFmt, token[0])
			if er != nil {
				log.Printf("Time parse error! %v  %v", tm, er)
			}
			dur := tm.Sub(last)
			//			log.Printf("Sleep %v %v %v",dur, tm, last)
			if dur.Nanoseconds() > 0 {
				if *speed > 0 {
					time.Sleep(time.Duration(float64(dur.Nanoseconds()) / *speed))
				}
				last = tm
			}
			if dur.Nanoseconds() < 0 {
				last = tm
			}

			//			tp, _ := ptypes.TimestampProto(tm)
			/*			ge.Ts = tp
						ge.Hostname = token[1]
						ge.DeviceId = token[2]
						ge.Mac = token[2]
						ge.Ip = token[3]
			*/
		}
	}

	if ge != nil {
		if len(evts) > 0 {
			if *multi == 1 { // sending each packets
				ge.Data = evts
				out, _ := proto.Marshal(ge)
				cont := pb.Content{Entity: out}
				smo := sxutil.SupplyOpts{
					Name:  "GridEye",
					Cdata: &cont,
				}
				_, nerr := client.NotifySupply(&smo)
				if nerr != nil {
					log.Printf("Send Fail! %v\n", nerr)
				} else {
					//							log.Printf("Sent OK! %#v\n", ge)
				}
			} else { // sending multiple packets
				mcount++
				ge.Data = evts
				ges = append(ges, ge)
				gess := &grideye.GridEyes{
					Messages: ges,
				}
				out, _ := proto.Marshal(gess)
				cont := pb.Content{Entity: out}
				smo := sxutil.SupplyOpts{
					Name:  "GridEyeMulti",
					Cdata: &cont,
				}
				_, nerr := client.NotifySupply(&smo)
				if nerr != nil {
					log.Printf("Send Fail! %v\n", nerr)
				} else {
					log.Printf("Sent Last OK! %d bytes: %s\n", len(out), ptypes.TimestampString(ge.Ts))
				}
			}
		}
	}
}

func sendAllGridEyeFile(client *sxutil.SXServiceClient) {
	// check all files in dir.
	stMonth, stDate := getMonthDate(*startDate)
	edMonth, edDate := getMonthDate(*endDate)

	if *dir == "" {
		log.Printf("Please specify directory")
		data := "data"
		dir = &data
	}
	files, err := ioutil.ReadDir(*dir)

	if err != nil {
		log.Printf("Can't open diretory %v", err)
		os.Exit(1)
	}
	// should be sorted.
	var ss = make(sort.StringSlice, 0, len(files))

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") { // check is CSV file
			//
			fn := file.Name()
			var year, month, date int
			ct, err := fmt.Sscanf(fn, "%4d-%02d-%02d.csv", &year, &month, &date)
			if (month > stMonth || (month == stMonth && date >= stDate)) &&
				(month < edMonth || (month == edMonth && date <= edDate)) {
				ss = append(ss, file.Name())
			} else {
				log.Printf("file: %d %v %s: %04d-%02d-%02d", ct, err, fn, year, month, date)
			}
		}
	}

	ss.Sort()

	for _, fname := range ss {
		dfile := path.Join(*dir, fname)
		// check start date.

		log.Printf("Sending %s", dfile)
		sendfile = &dfile
		sendingGridEyeFile(client)
	}

}

//dataServer(geClient)

func main() {
	log.Printf("GridEyeRetrieve(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.GRIDEYE_SVC}

	srv, rerr := sxutil.RegisterNode(*nodesrv, "GridEyeRetrieve", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		srv = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", srv)

	//	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(srv)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	geClient := sxutil.NewSXServiceClient(client, pbase.GRIDEYE_SVC, "{Client:GridEyeRetrieve}")

	//	wg.Add(1)
	//    log.Print("Subscribe Supply")
	//    go subscribeGridEyeSupply(geClient)

	if *all { // send all file
		sendAllGridEyeFile(geClient)
	} else if *dir != "" {
	} else if *sendfile != "" {
		//		for { // infinite loop..
		sendingGridEyeFile(geClient)
		//		}
	}

	//	wg.Wait()

}
