# BoltDB storage for Colly

A [BoltDB](https://github.com/boltdb/bolt) storage back-end for the
[Colly](https://go-colly.org) web crawling/scraping framework.

It implements both the storage and queue interfaces.

Example Usage:

```go
package main

import (
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/debug"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/gocolly/colly/v2/proxy"
	"github.com/gocolly/colly/v2/queue"
	bolt "src.userspace.com.au/colly-bolt-storage"
)

func main() {
	c := colly.NewCollector(
		colly.AllowedDomains("www.example.com"),
	)

	storage, err := bolt.New("./state.bdb")
	if err != nil {
		panic(err)

	}
	defer storage.Close()

	err := c.SetStorage(storage)
	if err != nil {
		panic(err)
	}

	// ...
}

```
