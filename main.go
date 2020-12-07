package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	chart "github.com/wcharczuk/go-chart/v2"
)

func setupTempDB() (string, error) {
	os.TempDir()
	path := fmt.Sprintf("%v/tempdb-%d", os.TempDir(), rand.Int31())
	if err := os.Mkdir(path, 0777); err != nil {
		return "", err
	}
	log.Info("Created temp db", "path", path)
	return path, nil

}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: headerimport <ancient-dir> <count>\n")
		os.Exit(1)
	}
	ancientDir := os.Args[1]
	var count int
	if c, err := strconv.Atoi(os.Args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "Count must be numeric, could not parse: %v\n", err)
		os.Exit(1)
	} else {
		count = c
	}
	// Set up logging
	glogger := log.NewGlogHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(false)))
	glogger.Verbosity(log.LvlInfo)
	log.Root().SetHandler(glogger)
	rand.Seed(time.Now().Unix())
	if err := doImport(ancientDir, count); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
func doImport(ancientDir string, count int) error {
	var db ethdb.Database
	// Create temporary database
	path, err := setupTempDB()
	if err != nil {
		return fmt.Errorf("Failed creating temp db: %v", err)
	}
	defer func() {
		log.Info("Cleaning temp db", "path", path)
		os.RemoveAll(path)
	}()
	if db, err = rawdb.NewLevelDBDatabaseWithFreezer(path, 100, 500_000, fmt.Sprintf("%v/ancients", path), "foo"); err != nil {
		return fmt.Errorf("Failed creating temp db: %v", err)
	}
	defer db.Close()
	// Open ancients
	freezer, err := rawdb.NewFreezerTable(ancientDir, "headers", false)
	if err != nil {
		return fmt.Errorf("Failed opening ancients: %v", err)
	}
	// Chan which we feed chunks of header from ancients to the db
	headerCh := make(chan []*types.Header)
	closeCh := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		readLoop(1, uint64(1+count), freezer, headerCh, closeCh)
		wg.Done()
	}()
	go func() {
		writeLoop(db, headerCh, closeCh)
		wg.Done()
	}()
	// wait for exit
	go func() {
		abortChan := make(chan os.Signal, 1)
		signal.Notify(abortChan, os.Interrupt)
		sig := <-abortChan
		log.Info("Exiting...", "signal", sig)
		close(closeCh)
	}()
	wg.Wait()
	return nil
}

type Retriever interface {
	Retrieve(item uint64) ([]byte, error)
}

const chunkSize = 2048

func readLoop(from, to uint64, freezer Retriever, headerCh chan []*types.Header, closeCh chan bool) {
	var chunk = make([]*types.Header, 0, chunkSize)
	for i := from; i < to; i++ {
		data, err := freezer.Retrieve(i)
		if err != nil {
			log.Error("Read error: %v", err)
			return
		}
		var h = new(types.Header)
		rlp.DecodeBytes(data, h)
		chunk = append(chunk, h)
		if len(chunk) >= chunkSize {
			var hdrs = make([]*types.Header, chunkSize)
			copy(hdrs, chunk)
			chunk = chunk[:0]
			select {
			case headerCh <- hdrs:
			case <-closeCh:
				return
			}
		}
	}
	close(headerCh)
	return
}

func writeLoop(db ethdb.Database, headerCh chan []*types.Header, closeCh chan bool) {
	engine := ethash.New(ethash.Config{
		CacheDir:         "./ethash-caches",
		CachesInMem:      eth.DefaultConfig.Ethash.CachesInMem,
		CachesOnDisk:     eth.DefaultConfig.Ethash.CachesOnDisk,
		CachesLockMmap:   eth.DefaultConfig.Ethash.CachesLockMmap,
		DatasetDir:       "./ethash-dag",
		DatasetsInMem:    eth.DefaultConfig.Ethash.DatasetsInMem,
		DatasetsOnDisk:   eth.DefaultConfig.Ethash.DatasetsOnDisk,
		DatasetsLockMmap: eth.DefaultConfig.Ethash.DatasetsLockMmap,
	}, nil, false)
	var interrupt int32
	// insertStopped returns true after StopInsert has been called.
	var insertStopped = func() bool {
		return atomic.LoadInt32(&interrupt) == 1
	}
	if _, _, err := core.SetupGenesisBlock(db, core.DefaultGenesisBlock()); err != nil {
		log.Error("Could not setup genesis", "error", err)
		return
	}
	hc, err := core.NewHeaderChain(db, params.MainnetChainConfig, engine, insertStopped)
	if err != nil {
		log.Error("Could not create chain", "error", err)
		return
	}
	var validationTime time.Duration
	var writeTime time.Duration
	var headersWritten uint64

	var xVals []float64
	var writeTimes []float64
	var validationTimes []float64
	// Needed on 'master'
	whFunc := func(header *types.Header) error {
		_, err := hc.WriteHeader(header)
		return err
	}
	for {
		select {
		case hdrs := <-headerCh:
			if hdrs == nil {
				render(xVals, writeTimes, validationTimes, "times")
				return
			}
			t0 := time.Now()
			if _, err := hc.ValidateHeaderChain(hdrs, 100); err != nil {
				log.Error("Validation error", "error", err)
				return
			}
			t1 := time.Now()
			//if _, err := hc.InsertHeaderChain(hdrs, t1); err != nil {

			if _, err := hc.InsertHeaderChain(hdrs, whFunc, t1); err != nil {
				log.Error("Write error", "error", err)
				return
			}
			t2 := time.Now()
			vTime := t1.Sub(t0)
			wTime := t2.Sub(t1)
			headersWritten += uint64(len(hdrs))
			validationTime += vTime
			writeTime += wTime
			xVals = append(xVals, float64(headersWritten))
			validationTimes = append(validationTimes, float64(validationTime/time.Millisecond))
			writeTimes = append(writeTimes, float64(writeTime/time.Millisecond))
			log.Info("Wrote headers", "count", len(hdrs), "vtime", vTime,
				"wtime", wTime, "all", headersWritten, "totV", validationTime,
				"totW", writeTime)
		case <-closeCh:
			return
		}
	}
}

func render(xvalues, writes, validations []float64, name string) {
	graph := chart.Chart{
		Series: []chart.Series{
			chart.ContinuousSeries{
				Name: "Write time (ms)",
				Style: chart.Style{
					StrokeColor: chart.GetDefaultColor(0), //.WithAlpha(64),
					FillColor:   chart.GetDefaultColor(0).WithAlpha(64),
				},
				XValues: xvalues,
				YValues: writes,
			},
			chart.ContinuousSeries{
				Name: "Validation time (ms)",
				Style: chart.Style{
					StrokeColor: chart.GetDefaultColor(1), //.WithAlpha(64),
					FillColor:   chart.GetDefaultColor(1).WithAlpha(64),
				},
				XValues: xvalues,
				YValues: validations,
			},
		},
	}
	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}
	fName := fmt.Sprintf("%v.png", name)
	f, _ := os.Create(fName)
	defer f.Close()
	graph.Render(chart.PNG, f)
	log.Info("Rendered", "file", fName)
}
