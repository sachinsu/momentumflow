package app

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/jackc/pgx/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	maxWorkers = 5
)

func AddCompaniesToDB(ctx context.Context, dbConn string, filePath string) error {
	conn, err := pgx.Connect(ctx, dbConn)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to read the file: %w", err)
	}

	defer file.Close()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("conn.Begin failed: %w", err)
	}

	defer func(ctx context.Context) {
		if err != nil {
			tx.Rollback(ctx)
		}
		tx.Commit(ctx)
	}(ctx)

	batch := &pgx.Batch{}

	batch.Queue("delete from cnx500companies")

	r := csv.NewReader(file)
	r.Read()

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed while parsing CSV %w", err)
		}
		// fmt.Printf("%v\n", record)
		batch.Queue("insert into cnx500companies(company,industry,symbol,series,isin) values($1,$2,$3,$4,$5)", record[0], record[1], record[2], record[3], record[4])

		if batch.Len()%100 == 0 {
			br := tx.SendBatch(ctx, batch)
			if _, err = br.Exec(); err != nil {
				return fmt.Errorf("failed while inserting batch %w", err)
			}
			br.Close()
			batch = &pgx.Batch{}
		}
	}

	if batch.Len() > 0 {
		br := tx.SendBatch(ctx, batch)

		if _, err = br.Exec(); err != nil {
			return fmt.Errorf("failed while inserting batch %w", err)
		}
		br.Close()
	}
	return nil
}

func CalculateNearYearlyHigh(ctx context.Context, dbConn string) error {
	var stocklist []Stockdata
	conn, err := pgx.Connect(ctx, dbConn)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "select symbol from cnx500companies")
	if err != nil {
		return fmt.Errorf("Error reading rows from table: %w", err)
	}
	defer rows.Close()
	g, ctxNew := errgroup.WithContext(ctx)

	c := make(chan Stockdata)

	sem := semaphore.NewWeighted(maxWorkers)

	go func() {
		for stock := range c {
			fmt.Printf("%s\n", stock.Symbol)
			stocklist = append(stocklist, stock)
		}
	}()

	for rows.Next() {
		var symbol string
		rows.Scan(&symbol)
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}

		// stock, err := GetStockData(ctx, symbol+".NS")
		// if err != nil {
		// 	return fmt.Errorf("Error retrieving stock price %w", err)
		// }
		// stock.Symbol = symbol
		// stocklist = append(stocklist, stock)

		g.Go(func() (err error) {
			defer sem.Release(1)

			stock, err := GetStockData(ctxNew, symbol+".NS")
			if err != nil {
				return err
			}
			stock.Symbol = symbol
			c <- stock
			// fmt.Printf("%s done\n", symbol)
			return err
		})
	}

	g.Wait()
	close(c)

	rows.Close()

	err = updateStockValues(ctx, stocklist, conn)
	if err != nil {
		return fmt.Errorf("Error in Updatestockvalues : %w", err)
	}
	return nil
}

func GetTopStocks(ctx context.Context, dbConn string) ([]Stockdata, error) {
	var stocks []Stockdata

	conn, err := pgx.Connect(ctx, dbConn)
	if err != nil {
		return stocks, fmt.Errorf("Unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "select company,symbol,ltp from ( select company,symbol,ltp, yearlyhigh-ltp closer from cnx500companies where ltp::money::numeric::float8 > 20 and ltp::money::numeric::float8 < 50000 ) tab order by closer limit 20")
	if err != nil {
		return stocks, fmt.Errorf("Error fetching top companies %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var company string
		var symbol string
		var ltp string

		err := rows.Scan(&company, &symbol, &ltp)

		if err != nil {
			return stocks, fmt.Errorf("Error while parsing data from DB %w", err)
		}

		ltpVal, _ := strconv.ParseFloat(ltp, 32)
		stocks = append(stocks, Stockdata{Symbol: fmt.Sprintf("%s - (%s)", company, symbol), Ltp: ltpVal})

	}

	return stocks, nil

}

// func IsClosed(ch <-chan T) bool {
// 	select {
// 	case <-ch:
// 		return true
// 	default:
// 	}

// 	return false
// }

func updateStockValues(ctx context.Context, stocks []Stockdata, conn *pgx.Conn) error {

	batch := &pgx.Batch{}

	for _, stock := range stocks {
		// fmt.Printf("%v\n", stock)
		batch.Queue("Update cnx500companies set ltp=$1, yearlyhigh=$2 where symbol=$3", stock.Ltp, stock.YearlyHigh, stock.Symbol)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("conn.Begin failed: %w", err)
	}

	defer func(ctx context.Context) {
		if err != nil {
			tx.Rollback(ctx)
		}
		tx.Commit(ctx)
	}(ctx)

	br := tx.SendBatch(ctx, batch)
	_, err = br.Exec()
	if err != nil {
		return fmt.Errorf("failed while updating records %w", err)
	}
	br.Close()
	return nil
}
