// **********************************************************************
//    Copyright (c) 2018 Henry Seurer
//
//   Permission is hereby granted, free of charge, to any person
//    obtaining a copy of this software and associated documentation
//    files (the "Software"), to deal in the Software without
//    restriction, including without limitation the rights to use,
//    copy, modify, merge, publish, distribute, sublicense, and/or sell
//    copies of the Software, and to permit persons to whom the
//    Software is furnished to do so, subject to the following
//    conditions:
//
//   The above copyright notice and this permission notice shall be
//   included in all copies or substantial portions of the Software.
//
//    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
//    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
//    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//    OTHER DEALINGS IN THE SOFTWARE.
//
// **********************************************************************

package redisdb

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
	"os/signal"
	"syscall"
)

type RedisDatabase struct {
	redisPool *redis.Pool
}

func (d *RedisDatabase) Ping() error {

	conn := d.redisPool.Get()
	defer conn.Close()

	_, err := redis.String(conn.Do("PING"))
	if err != nil {
		return fmt.Errorf("cannot 'PING' db: %v", err)
	}
	return nil
}

func (d *RedisDatabase) Get(key string) ([]byte, error) {

	conn := d.redisPool.Get()
	defer conn.Close()

	var data []byte
	data, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}
	return data, err
}

func (d *RedisDatabase) Set(key string, value []byte) error {

	conn := d.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	if err != nil {
		v := string(value)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}
	return err
}

func (d *RedisDatabase) Exists(key string) (bool, error) {

	conn := d.redisPool.Get()
	defer conn.Close()

	ok, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return ok, fmt.Errorf("error checking if key %s exists: %v", key, err)
	}
	return ok, err
}

func (d *RedisDatabase) Delete(key string) error {

	conn := d.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func (d *RedisDatabase) GetKeys(pattern string) ([]string, error) {

	conn := d.redisPool.Get()
	defer conn.Close()

	iter := 0
	var keys []string
	for {
		arr, err := redis.Values(conn.Do("SCAN", iter, "MATCH", pattern))
		if err != nil {
			return keys, fmt.Errorf("error retrieving '%s' keys", pattern)
		}

		iter, _ = redis.Int(arr[0], nil)
		k, _ := redis.Strings(arr[1], nil)
		keys = append(keys, k...)

		if iter == 0 {
			break
		}
	}

	return keys, nil
}

// Some redis functions, such as HMGET, return a slice of strings that correspond to a
// supplied slice of field names (keys). This splices those two slices into a map. It passes
// through an error value, similar to redigo’s convenience conversion functions, so it can be used
// with a minimum of ceremony.
func (d *RedisDatabase) spliceMap(keys []string, values []string, err error) (map[string]string, error) {
	if keys == nil || values == nil {
		if err == nil {
			return nil, fmt.Errorf("redis: cannot splice keys supplied to HMGET with values returned because one or both slices are nil")
		}
		return nil, err
	}

	lenKeys := len(keys)
	lenValues := len(values)

	if lenKeys != lenValues {
		if err == nil {
			return nil, fmt.Errorf("redis: cannot splice keys supplied to HMGET with values returned because their lengths are different")
		}
		return nil, err
	}

	result := map[string]string{}
	for i := 0; i < lenKeys; i++ {
		result[keys[i]] = values[i]
	}
	return result, err
}

func (d *RedisDatabase) HMGet(key string, fields ...string) (map[string]string, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("redis: at least once field is required")
	}

	conn := d.redisPool.Get()
	defer conn.Close()

	values, err := redis.Strings(conn.Do("HMGET", redis.Args{key}.AddFlat(fields)...))
	return d.spliceMap(fields, values, err)
}

func (d *RedisDatabase) HMGetKeys(key string) []string {
	conn := d.redisPool.Get()
	defer conn.Close()

	values, _ := redis.Strings(conn.Do("HKEYS", key))
	return values
}

func (d *RedisDatabase) HMGetAll(key string) map[string]string {
	conn := d.redisPool.Get()
	defer conn.Close()

	values, _ := redis.StringMap(conn.Do("HGETALL", key))
	return values
}

func (d *RedisDatabase) HMSet(key string, hashKey string, value []byte) error {
	conn := d.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("HMSET", key, hashKey, value)
	if err != nil {
		v := string(value)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf("error setting key %s:%s to %s: %v", key, hashKey, v, err)
	}
	return err
}

func (d *RedisDatabase) Incr(counterKey string) (int, error) {

	conn := d.redisPool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("INCR", counterKey))
}

var (
	gRedisPool *redis.Pool
)

func newPool(redisURL string) *redis.Pool {
	return &redis.Pool{
		// Maximum number of idle connections in the redisPool.
		MaxIdle: 80,
		// max number of connections
		MaxActive: 12000,
		// Dial is an application supplied function for creating and
		// configuring a connection.
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(redisURL)
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

func cleanupHook() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGKILL)
	go func() {
		<-c
		gRedisPool.Close()
		os.Exit(0)
	}()
}

func SetupDatabase(redisURL string) {
	gRedisPool = newPool(redisURL)
	cleanupHook()
}

func GetDatabase() RedisDatabase {
	return RedisDatabase{redisPool: gRedisPool}
}
