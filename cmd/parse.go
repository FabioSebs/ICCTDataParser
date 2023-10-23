/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/FabioSebs/ICCTDataParser/entities"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/xuri/excelize/v2"
)

var (
	client *elasticsearch.Client
	// err    error
)

// parseCmd represents the parse command
var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "parse your excel file into json and store into kibana",
	Long:  `parse your excel file into json and store into kibana`,
	Run: func(cmd *cobra.Command, args []string) {
		// flag
		fname, err := cmd.Flags().GetString("fname")
		if err != nil || fname == "" {
			fmt.Println("make sure to specify the file name using --fname")
		}

		ftype := strings.Split(fname, ".")

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////HANDLE PARSING////////////////////////////////////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		xlFile, err := excelize.OpenFile(fname)
		if err != nil {
			fmt.Println("error opening the excel file:", err)
			return
		}
		defer xlFile.Close()

		// Get the sheet name
		sheet := xlFile.GetSheetName(0)

		// Read the data row by row
		rows, err := xlFile.GetRows(sheet)
		if err != nil {
			fmt.Println("error opening the excel file:", err)
			return
		}
		//elasticsearch client
		caCert, err := ioutil.ReadFile("http_ca.crt")
		if err != nil {
			fmt.Println(err)
			return
		}

		client, err = elasticsearch.NewClient(elasticsearch.Config{ // client
			Addresses:         []string{os.Getenv("es.address")},
			Username:          os.Getenv("es.username"),
			Password:          os.Getenv("es.password"),
			EnableDebugLogger: true,
			CACert:            caCert,
		})
		if err != nil {
			panic(err)
		}

		var datalist []entities.BaselineData

		for _, row := range rows[1:] { // Skip the header row
			baselinedata := entities.BaselineData{}

			// Extract values from each cell
			country := row[0]
			year, _ := strconv.Atoi(row[1])
			percentage, _ := strconv.ParseFloat(row[2], 64)

			// Create a struct instance and append it to the data slice
			baselinedata.Country = country
			baselinedata.Year = year
			baselinedata.Percentage = float32(percentage)

			// append to datalist
			if baselinedata.Country != "" || baselinedata.Year != 0 || baselinedata.Percentage != 0 {
				datalist = append(datalist, baselinedata)
			}
		}

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		///////////////////////////////////////////////////////////END PARSING//////////////////////////////////////////////////////////////////////////////////////////////
		///////////////////////////////////////////////////////////BEGIN ELASTIC///////////////////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// ElasticSearch Index
		for key, val := range datalist {
			data, err := json.Marshal(val)
			if err != nil {
				fmt.Printf("error parsing json data: %s", err)
				return
			}

			req := esapi.IndexRequest{
				Index:      "icct_" + ftype[0],
				DocumentID: uuid.New().String(),
				Body:       bytes.NewReader(data),
				Refresh:    "true",
			}

			res, err := req.Do(context.Background(), client)
			if err != nil {
				fmt.Printf("error indexing json data: %s", err)
				return
			}
			defer res.Body.Close()

			if res.IsError() {
				fmt.Printf("error indexing json data. status code: %d", res.StatusCode)
			}
			fmt.Println(key, val)
		}
		fmt.Println("succesfully added to elasticsearch :)")

	},
}

func init() {
	rootCmd.AddCommand(parseCmd)

	// Here you will define your flags and configuration settings.
	parseCmd.Flags().String("fname", "", "name of file, make sure youre in correct directory")
}
