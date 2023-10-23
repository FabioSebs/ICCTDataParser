package main

import (
	"log"

	"github.com/FabioSebs/ICCTDataParser/cmd"
	"github.com/joho/godotenv"
)

func main() {
	cmd.Execute()
}

func init() {
	// Load environment variables from the .env file
	if err := godotenv.Load(".env"); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
}
