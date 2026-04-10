package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/bluesgao/sharding"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	shardCount = 4
	dsn        = "host=localhost user=postgres password=postgres dbname=postgres port=5432 sslmode=disable"
)

type Order struct {
	ID        int64 `gorm:"primarykey"`
	OrderID   int64
	ProductID int64
}

type User struct {
	ID       int64 `gorm:"primarykey"`
	UserID   int64
	UserName string
}

func main() {
	db, err := openDB()
	if err != nil {
		log.Fatal(err)
	}

	resetShardTables(db, shardCount)

	mw := shardingMiddleware(shardCount)
	if err := db.Use(mw); err != nil {
		log.Fatalf("sharding plugin: %v", err)
	}

	if err := seed(db); err != nil {
		log.Fatal(err)
	}

	verify(db)
}

func openDB() (*gorm.DB, error) {
	return gorm.Open(postgres.New(postgres.Config{DSN: dsn}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
}

func resetShardTables(db *gorm.DB, n int) {
	for i := 0; i < n; i++ {
		t := fmt.Sprintf("orders_%02d", i)
		db.Exec(`DROP TABLE IF EXISTS ` + t)
		db.Exec(`CREATE TABLE ` + t + ` (
			id BIGSERIAL PRIMARY KEY,
			order_id bigint,
			product_id bigint
		)`)
	}
	for i := 0; i < n; i++ {
		t := fmt.Sprintf("users_%02d", i)
		db.Exec(`DROP TABLE IF EXISTS ` + t)
		db.Exec(`CREATE TABLE ` + t + ` (
			id BIGSERIAL PRIMARY KEY,
			user_id bigint,
			user_name varchar(255)
		)`)
	}
}

func modShardSuffix02d(n int) func(any) (string, error) {
	return func(value any) (string, error) {
		var id int
		switch v := value.(type) {
		case int:
			id = v
		case int64:
			id = int(v)
		default:
			return "", fmt.Errorf("shard key must be int or int64, got %T", value)
		}
		return fmt.Sprintf("_%02d", id%n), nil
	}
}

func shardSuffixes(n int) func() []string {
	return func() []string {
		s := make([]string, 0, n)
		for i := 0; i < n; i++ {
			s = append(s, fmt.Sprintf("_%02d", i))
		}
		return s
	}
}

func shardingMiddleware(n int) *sharding.Sharding {
	sfx := shardSuffixes(n)
	mod := modShardSuffix02d(n)
	base := sharding.Config{
		NumberOfShards:      uint(n),
		PrimaryKeyGenerator: sharding.PKSnowflake,
		ShardingAlgorithm:   mod,
		ShardingSuffixs:     sfx,
	}

	orderCfg := base
	orderCfg.ShardingKey = "order_id"

	userCfg := base
	userCfg.ShardingKey = "user_id"

	return sharding.RegisterGroups(
		sharding.TableGroup{Config: orderCfg, Tables: []any{"orders"}},
		sharding.TableGroup{Config: userCfg, Tables: []any{"users"}},
	)
}

func seed(db *gorm.DB) error {
	for i := 0; i < shardCount; i++ {
		if err := db.Create(&Order{OrderID: int64(i), ProductID: int64(i * 10)}).Error; err != nil {
			return fmt.Errorf("seed Order: %w", err)
		}
	}
	for i := 0; i < shardCount; i++ {
		if err := db.Create(&User{UserID: int64(i), UserName: fmt.Sprintf("u%d", i)}).Error; err != nil {
			return fmt.Errorf("seed User: %w", err)
		}
	}
	return nil
}

func verify(db *gorm.DB) {
	const orderKey int64 = 3 // → orders_03
	const userKey int64 = 2  // → users_02

	fmt.Println("--- orders ---")
	var o Order
	if err := db.Model(&Order{}).Where("order_id", orderKey).First(&o).Error; err != nil {
		fmt.Println("First:", err)
	} else {
		fmt.Printf("First: order_id=%d → orders_%02d id=%d product_id=%d\n",
			orderKey, orderKey%shardCount, o.ID, o.ProductID)
	}

	if err := db.Model(&Order{}).Where("order_id", orderKey).Update("product_id", 777).Error; err != nil {
		fmt.Println("Update:", err)
	} else {
		_ = db.Model(&Order{}).Where("order_id", orderKey).First(&o)
		fmt.Printf("Update: product_id=%d (want 777)\n", o.ProductID)
	}

	var raw Order
	if err := db.Raw("SELECT * FROM orders WHERE order_id = ?", orderKey).Scan(&raw).Error; err != nil {
		fmt.Println("Raw:", err)
	} else {
		fmt.Printf("Raw: order_id=%d product_id=%d\n", raw.OrderID, raw.ProductID)
	}

	phy := fmt.Sprintf("orders_%02d", orderKey%shardCount)
	var cnt int64
	if err := db.Raw("SELECT count(*) FROM " + phy).Scan(&cnt).Error; err != nil {
		fmt.Println("count physical:", err)
	} else {
		fmt.Printf("physical %s: count=%d\n", phy, cnt)
	}

	var orders []Order
	err := db.Model(&Order{}).Where("product_id", 777).Find(&orders).Error
	switch {
	case err == nil:
		fmt.Println("missing shard key: want error, got nil")
	case errors.Is(err, sharding.ErrMissingShardingKey):
		fmt.Println("missing shard key: ErrMissingShardingKey (ok)")
	default:
		fmt.Println("missing shard key:", err)
	}

	fmt.Println("--- users ---")
	var u User
	if err := db.Model(&User{}).Where("user_id", userKey).First(&u).Error; err != nil {
		fmt.Println("First:", err)
	} else {
		fmt.Printf("First: user_id=%d → users_%02d name=%s\n", userKey, userKey%shardCount, u.UserName)
	}
}
