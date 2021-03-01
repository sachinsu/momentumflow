package app

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	maxMomentumStocks = 10
	lowerLTPLimit     = 20
	upperLTPLimit     = 50000
	maxWorkers        = 5
)

func AddCompaniesToDB(ctx context.Context, dbConn string, filePath string) error {
	conn, err := pgx.Connect(ctx, dbConn)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	records, err := readCSVFromUrl(filePath)

	if err != nil {
		return fmt.Errorf("failed to read the file: %w", err)
	}

	// file, err := os.Open(filePath)

	// defer file.Close()

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

	// r := csv.NewReader(file)
	// r.Read()

	for idx, record := range records {
		if idx == 0 {
			continue
		}

		// fmt.Printf("%v--%d\n", record, len(record[0]))
		batch.Queue("insert into cnx500companies(company,industry,symbol) values($1,$2,$3)", record[0], record[1], record[2])

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

		if rows.Err() != nil {
			return rows.Err()
		}

		// fmt.Println(symbol)
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
			Symbol := symbol
			defer sem.Release(1)

			stock, err := GetStockData(ctxNew, Symbol+".NS")
			if err != nil {
				return err
			}
			stock.Symbol = Symbol
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

func GetMomentumStocks(ctx context.Context, dbConn string) ([]Stockdata, error) {
	var stocks []Stockdata

	conn, err := pgx.Connect(ctx, dbConn)
	if err != nil {
		return stocks, fmt.Errorf("Unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// Check for stocks whether they continue to be 'buy'
	_, err = conn.Exec(ctx, `insert into momentumstocks(company,symbol,ltp,buyorsell) select company,symbol,ltp,'buy' from cnx500companies  where exists
	(select symbol from momentumstocks where buyorsell=$1 and symbol=cnx500companies.symbol order by updatedat desc limit $2) and yearlyhigh-ltp < (5/100*yearlyhigh) 
	and  exists (select symbol from cnx500companies where symbol=cnx500companies.symbol order by yearlyhigh-ltp limit $2)`, "buy", maxMomentumStocks)

	if err != nil {
		return stocks, fmt.Errorf("Error inserting top companies %w", err)
	}

	tag, err := conn.Exec(ctx, ` insert into momentumstocks(company,symbol,ltp,buyorsell)
	select company,symbol,ltp,'buy'
	from cnx500companies 
	where not exists (select symbol 
			  from momentumstocks 
			  where buyorsell=$1 and updatedat=current_date)
	and ltp::money::numeric::float8 > $2 and ltp::money::numeric::float8 < $3
	order by yearlyhigh-ltp 
	limit (select $4-count(*) from momentumstocks where buyorsell=$1 and updatedat=current_date)
  `, "buy", lowerLTPLimit, upperLTPLimit, maxMomentumStocks)

	if err != nil {
		return stocks, fmt.Errorf("Error inserting top companies %w", err)
	}

	if tag.RowsAffected() < 1 {
		return stocks, fmt.Errorf("Unable to determine top n records")
	}
	// remove stocks if,
	// it is not in top 10 of this week or
	// it > 5% than their 52 week high

	rows, err := conn.Query(ctx, `select company,symbol,ltp,buyorsell 
								from momentumstocks
								where updatedat >= (select updatedat 
								from momentumstocks
								order by updatedat desc limit 1)`)
	if err != nil {
		return stocks, fmt.Errorf("Error fetching top companies %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var company string
		var symbol string
		var ltp string
		var buyorsell string

		err := rows.Scan(&company, &symbol, &ltp, &buyorsell)

		if err != nil {
			return stocks, fmt.Errorf("Error while parsing data from DB %w", err)
		}

		ltpVal, _ := strconv.ParseFloat(ltp, 32)
		stocks = append(stocks, Stockdata{Symbol: fmt.Sprintf("%s - (%s) - (%s)", company, symbol, buyorsell), Ltp: ltpVal})

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
