package main

import (
	"flag"
	"fmt"
	"log"
	"path"
	"strings"

	mapset "github.com/deckarep/golang-set"
	"github.com/fatih/color"
	"github.com/google/go-cmp/cmp"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	separator = " -> "
)

var (
	raw = flag.Bool("raw", false, "print raw bytes")
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

	log.Printf("Keys (%s): %d", args[0], leftKeys.Cardinality())

	log.Printf("Traversing %s keys...", args[1])
	rightKeys, err := walkKeys(right)
	if err != nil {
		return err
	}

	printDeleted(leftKeys, rightKeys)
	printAdded(leftKeys, rightKeys)
	if err = printModified(left, right, leftKeys, rightKeys); err != nil {
	log.Printf("Keys (%s): %d", args[1], rightKeys.Cardinality())
		return err
	}

	return nil
}

func printDeleted(leftKeys, rightKeys mapset.Set) {
	var deleted []string
	leftKeys.Difference(rightKeys).Each(func(elem interface{}) bool {
		deleted = append(deleted, elem.(string))
		return false
	})

	if len(deleted) > 0 {
		color.New(color.FgCyan, color.Bold).Print("Deleted:\n")
		red := color.New(color.FgRed)
		for _, d := range deleted {
			red.Printf("--- %s\n", d)
		}
	}
}

func printAdded(leftKeys, rightKeys mapset.Set) {
	var added []string
	rightKeys.Difference(leftKeys).Each(func(elem interface{}) bool {
		added = append(added, elem.(string))
		return false
	})

	if len(added) > 0 {
		color.New(color.FgCyan, color.Bold).Print("\nAdded:\n")
		green := color.New(color.FgGreen)
		for _, a := range added {
			green.Printf("+++ %s\n", a)
		}
	}
}

func printModified(left, right *bolt.DB, leftKeys, rightKeys mapset.Set) error {
	var err error
	color.New(color.FgCyan, color.Bold).Print("\nModified:\n")
	bold := color.New(color.Bold)
	leftKeys.Intersect(rightKeys).Each(func(elem interface{}) bool {
		var leftValue, rightValue []byte
		key := elem.(string)
		leftValue, err = getValue(left, key)
		if err != nil {
			return true
		}
		rightValue, err = getValue(right, key)
		if err != nil {
			return true
		}

		var v1, v2 interface{} = leftValue, rightValue
		if !*raw {
			v1, v2 = string(leftValue), string(rightValue)
		}

		if diff := cmp.Diff(v1, v2); diff != "" {
			bold.Printf("diff a/%s b/%s\n", key, key)
			bold.Printf("--- a/%s\n", key)
			bold.Printf("--- b/%s\n", key)
			fmt.Println(diff)
		}
		return false
	})
	if err != nil {
		return err
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

func walkBucket(b *bolt.Bucket, buckets []string) (mapset.Set, error) {
	keys := mapset.NewSet()
	err := b.ForEach(func(k, v []byte) error {
		// k is a bucket
		if v == nil {
			nestedKeys, err := walkBucket(b.Bucket(k), append(buckets, string(k)))
			if err != nil {
				return err
			}
			keys = keys.Union(nestedKeys)
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

func walkKeys(db *bolt.DB) (mapset.Set, error) {
	keys := mapset.NewSet()
	err := db.View(func(tx *bolt.Tx) error {
		g, ctx := errgroup.WithContext(context.Background())
		sem := semaphore.NewWeighted(20)
		done := make(chan struct{})

		keysChan := make(chan mapset.Set)

		go func() {
			for k := range keysChan {
				keys = keys.Union(k)
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

