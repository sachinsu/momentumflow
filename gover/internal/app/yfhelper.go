package app

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/antchfx/htmlquery"
)

type Stockdata struct {
	Symbol     string
	Ltp        float64
	YearlyHigh float64
}

func readCSVFromUrl(url string) ([][]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetStockData - parses stock data for symbol from yahoo finance
func GetStockData(ctx context.Context, symbol string) (Stockdata, error) {
	stockdata := Stockdata{}
	doc, err := htmlquery.LoadURL(fmt.Sprintf("https://finance.yahoo.com/quote/%s", symbol))

	if err != nil {
		// log.Fatalf("Error while loading URL %v , %v ", err, ctx)
		return stockdata, fmt.Errorf("Error while loading URL: %s-> %w", symbol, err)
	}

	var valu float64

	ltp := htmlquery.FindOne(doc, "//*[@id='quote-header-info']/div[3]/div[1]/div/span[1]")
	if valu, err = strconv.ParseFloat(strings.Replace(htmlquery.InnerText(ltp), ",", "", -1), 32); err != nil {
		return stockdata, fmt.Errorf("Error while parsing LTP %w", err)
	}

	stockdata.Ltp = valu

	ltp = htmlquery.FindOne(doc, "//*[@id='quote-summary']/div[1]/table/tbody/tr[6]/td[2]")

	if valu, err = strconv.ParseFloat(strings.TrimSpace(strings.Replace(strings.Split(htmlquery.InnerText(ltp), "-")[1], ",", "", -1)), 32); err != nil {
		return stockdata, fmt.Errorf("Error while parsing 52week high price %w", err)
	}
	stockdata.YearlyHigh = valu
	// fmt.Printf("Stock done: %s\n", symbol)

	return stockdata, nil

}
