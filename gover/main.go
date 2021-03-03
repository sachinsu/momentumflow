package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/sachinsu/gopgstocks/internal/app"
)

const (
	exitFail = 1
)

func main() {

	// import data into table from CSV
	// for batch of n rows, get LTP and 52week high
	// Order by least difference between two
	if err := run(os.Args, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(exitFail)
	}
}

func run(args []string, stdout io.Writer) error {

	flags := flag.NewFlagSet(args[0], flag.ExitOnError)

	var importfilepath string

	flags.StringVar(&importfilepath, "importfile", "https://www1.nseindia.com/content/indices/ind_nifty500list.csv", "URL of file containing list of stocks in index")

	// cpuprofile := flags.String("cpuprofile", "", "write cpu profile to file")

	if err := flags.Parse(args[1:]); err != nil {
		return err
	}

	// if *cpuprofile != "" {
	// 	f, err := os.Create(*cpuprofile)
	// 	if err != nil {
	// 		_, _ = fmt.Fprintf(os.Stderr, "can't create profiler: %s", err.Error())
	// 	}
	// 	err = pprof.StartCPUProfile(f)
	// 	if err != nil {
	// 		_, _ = fmt.Fprintf(os.Stderr, "can't start profiler: %s", err.Error())
	// 	}
	// 	defer pprof.StopCPUProfile()
	// }

	//ref: https://medium.com/honestbee-tw-engineer/gracefully-shutdown-in-go-http-server-5f5e6b83da5a
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		dbconn := "host=127.0.0.1 port=5432 user=momentumflow password=momentumflow dbname=momentumflow sslmode=disable"

		err := app.AddCompaniesToDB(ctx, dbconn, importfilepath)
		if err != nil {
			fmt.Printf("Step 1 Error : %v\n", err)
			return
		}

		fmt.Println("Adding Companies to DB..done")
		err = app.CalculateNearYearlyHigh(ctx, dbconn)
		if err != nil {
			fmt.Printf("Step 2 Error : %v\n", err)
			return
		}

		fmt.Println("Calculating stockwise Momentum ..done")
		// stocks, err := app.GetMomentumStocks(ctx, dbconn)
		// if err != nil {
		// 	fmt.Printf("Step 3 Error : %v\n", err)
		// 	return
		// } else {
		// 	fmt.Println("Top 20 Momentum stocks are")
		// 	for _, v := range stocks {
		// 		fmt.Printf("%s\n", v.Symbol)
		// 	}
		// }
		fmt.Println("Done!!")

	}()

	fmt.Printf("Application Started\n")

	<-done
	fmt.Println("Application Stopped")

	defer func() {
		// extra handling here
		cancel()
	}()

	return nil
}
