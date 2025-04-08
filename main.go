package main

import (
	"flag"
	"log"

	"tsv-cleaner/internal/processor"
)

func main() {
	in := flag.String("in", "", "Input TSV file path")
	out := flag.String("out", "", "Output cleaned TSV file path")
	workers := flag.Int("workers", 4, "Number of concurrent workers")
	flag.Parse()

	if *in == "" || *out == "" {
		log.Fatal("Both -in and -out are required")
	}

	if err := processor.CleanTSV(*in, *out, *workers); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
