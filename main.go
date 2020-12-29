package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-set/strset"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	separator = " -> "
)

var (
	raw          = flag.Bool("raw", false, "print raw bytes")
	summary      = flag.Bool("summary", false, "print summary")
	skipAdded    = flag.Bool("skip-added", false, "suppress added keys")
	skipDeleted  = flag.Bool("skip-deleted", false, "suppress deleted keys")
	skipModified = flag.Bool("skip-modified", false, "suppress modified items")
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		fmt.Println("Usage: boltdiff DB1 DB2")
		return nil
	}

	options := &bolt.Options{ReadOnly: true}
	log.Printf("Opening %s...", args[0])
	left, err := bolt.Open(args[0], 0600, options)
	if err != nil {
		return err
	}
	defer left.Close()

	log.Printf("Opening %s...", args[1])
	right, err := bolt.Open(args[1], 0600, options)
	if err != nil {
		return err
	}
	defer right.Close()

	log.Printf("Traversing %s keys...", args[0])
	leftKeys, err := walkKeys(left)
	if err != nil {
		return err
	}

	log.Printf("Keys (%s): %d", args[0], leftKeys.Size())

	log.Printf("Traversing %s keys...", args[1])
	rightKeys, err := walkKeys(right)
	if err != nil {
		return err
	}

	log.Printf("Keys (%s): %d", args[1], rightKeys.Size())

	if !*skipDeleted {
		printDeleted(leftKeys, rightKeys)
	}
	if !*skipAdded {
		printAdded(leftKeys, rightKeys)
	}
	if !*skipModified {
		if err = printModified(args[0], args[1], left, right, leftKeys, rightKeys); err != nil {
			return err
		}
	}

	return nil
}

func printDeleted(leftKeys, rightKeys *strset.Set) {
	var deleted []string
	strset.Difference(leftKeys, rightKeys).Each(func(key string) bool {
		deleted = append(deleted, key)
		return true
	})
	sort.Slice(deleted, func(i, j int) bool {
		return deleted[i] < deleted[j]
	})

	if len(deleted) > 0 {
		color.New(color.FgCyan, color.Bold).Printf("Deleted: %d\n", len(deleted))
		if *summary {
			return
		}
		red := color.New(color.FgRed)
		for _, d := range deleted {
			red.Printf("--- %s\n", d)
		}
	}
}

func printAdded(leftKeys, rightKeys *strset.Set) {
	var added []string
	strset.Difference(rightKeys, leftKeys).Each(func(key string) bool {
		added = append(added, key)
		return true
	})
	sort.Slice(added, func(i, j int) bool {
		return added[i] < added[j]
	})

	if len(added) > 0 {
		color.New(color.FgCyan, color.Bold).Printf("\nAdded: %d\n", len(added))
		if *summary {
			return
		}
		green := color.New(color.FgGreen)
		for _, a := range added {
			green.Printf("+++ %s\n", a)
		}
	}
}

type modified struct {
	key  string
	diff string
}

func printModified(leftPath, rightPath string, left, right *bolt.DB, leftKeys, rightKeys *strset.Set) error {
	var err error
	var modifiedItems []modified

	bold := color.New(color.Bold)

	strset.Intersection(leftKeys, rightKeys).Each(func(key string) bool {
		var leftValue, rightValue []byte
		leftValue, err = getValue(left, key)
		if err != nil {
			return false
		}
		rightValue, err = getValue(right, key)
		if err != nil {
			return false
		}

		var v1, v2 interface{} = leftValue, rightValue
		if !*raw {
			v1, v2 = string(leftValue), string(rightValue)
		}

		if diff := cmp.Diff(v1, v2); diff != "" {
			modifiedItems = append(modifiedItems, modified{
				key:  key,
				diff: diff,
			})
		}
		return true
	})
	if err != nil {
		return err
	}

	color.New(color.FgCyan, color.Bold).Printf("\nModified: %d\n", len(modifiedItems))
	if *summary {
		return nil
	}
	for _, m := range modifiedItems {
		bold.Printf("diff a/%s b/%s\n", leftPath, rightPath)
		bold.Printf("--- a/%s\n", m.key)
		bold.Printf("+++ b/%s\n", m.key)
		fmt.Println(m.diff)
	}
	return nil
}

func getValue(db *bolt.DB, key string) ([]byte, error) {
	var value []byte
	keys := strings.Split(key, separator)
	buckets, key := keys[:len(keys)-1], keys[len(keys)-1]
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(buckets[0]))
		for _, b := range buckets[1:] {
			if b == "" {
				break
			}
			bucket = bucket.Bucket([]byte(b))
		}
		value = bucket.Get([]byte(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func walkBucket(b *bolt.Bucket, buckets []string) (*strset.Set, error) {
	keys := strset.New()
	err := b.ForEach(func(k, v []byte) error {
		// k is a bucket
		if v == nil {
			nestedKeys, err := walkBucket(b.Bucket(k), append(buckets, string(k)))
			if err != nil {
				return err
			}
			keys.Merge(nestedKeys)
			return nil
		}

		key := strings.Join(append(buckets, string(k)), separator)
		keys.Add(key)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func walkKeys(db *bolt.DB) (*strset.Set, error) {
	keys := strset.New()
	err := db.View(func(tx *bolt.Tx) error {
		g, ctx := errgroup.WithContext(context.Background())
		sem := semaphore.NewWeighted(20)
		done := make(chan struct{})

		keysChan := make(chan *strset.Set)

		go func() {
			for k := range keysChan {
				keys.Merge(k)
			}
			done <- struct{}{}
		}()

		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			g.Go(func() error {
				defer sem.Release(1)

				log.Printf("    Bucket: %s", string(name))
				bucketKeys, err := walkBucket(b, []string{string(name)})
				if err != nil {
					return err
				}
				keysChan <- bucketKeys
				return nil
			})
			return nil
		})
		if err != nil {
			return err
		}
		if err := g.Wait(); err != nil {
			return err
		}
		close(keysChan)
		<-done

		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}
