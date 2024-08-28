package internal

import (
	"context"
	"path/filepath"
	"strconv"
	"time"

	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// example:
// [postgres]
// host = "localhost"
// port = 5432
// user = "
// password = ""
// database = ""

type PostgresConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}

func config() *pgxpool.Config {
	const defaultMaxConns = int32(32)
	const defaultMinConns = int32(0)
	const defaultMaxConnLifetime = time.Hour
	const defaultMaxConnIdleTime = time.Minute * 30
	const defaultHealthCheckPeriod = time.Minute
	const defaultConnectTimeout = time.Second * 5

	// Your own Database URL
	var pgConnectConfig = loadPgConfig()
	var connectionUrl = "postgres://" + pgConnectConfig.User + ":" + pgConnectConfig.Password + "@" + pgConnectConfig.Host + ":" + strconv.Itoa(pgConnectConfig.Port) + "/" + pgConnectConfig.Database

	dbConfig, err := pgxpool.ParseConfig(connectionUrl)
	if err != nil {
		log.Fatal().Msgf("Failed to create a config, error: %v", err)
	}

	dbConfig.MaxConns = defaultMaxConns
	dbConfig.MinConns = defaultMinConns
	dbConfig.MaxConnLifetime = defaultMaxConnLifetime
	dbConfig.MaxConnIdleTime = defaultMaxConnIdleTime
	dbConfig.HealthCheckPeriod = defaultHealthCheckPeriod
	dbConfig.ConnConfig.ConnectTimeout = defaultConnectTimeout

	dbConfig.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
		log.Debug().Msg("Before acquiring the connection pool to the database!!")
		return true
	}

	dbConfig.AfterRelease = func(c *pgx.Conn) bool {
		log.Debug().Msg("After releasing the connection pool to the database!!")
		return true
	}

	dbConfig.BeforeClose = func(c *pgx.Conn) {
		log.Info().Msg("Closed the connection pool to the database!!")
	}

	return dbConfig
}

func loadPgConfig() *PostgresConfig {
	var (
		_, b, _, _ = runtime.Caller(0)

		// Root folder of this project
		Root = filepath.Join(filepath.Dir(b), "..")
	)
	var config PostgresConfig
	_, err := toml.DecodeFile(filepath.Join(Root, "config.toml"), &config)
	if err != nil {
		log.Fatal().Msgf("Failed to load config, error: %v", err)
	}
	println("Loaded config: ", config.Host)

	return &config

}

func Connect() (*pgxpool.Pool, error) {
	db, err := pgxpool.NewWithConfig(context.Background(), config())
	if err != nil {
		log.Fatal().Msgf("Failed to connect to the database, error: %v ", err)
	}

	return db, err
}

func InsertOneContract(db *pgxpool.Pool, record ContractDeployRecord) error {
	conn, err := db.Acquire(context.Background())
	if err != nil {
		log.Fatal().Msgf("Failed to acquire a connection, error: %v", err)
		return err
	}
	_, err = conn.Exec(context.Background(), "INSERT INTO contract.contract_deploy_record (contract_address, deployer, codehash, creation_time, txhash, block_num) VALUES($1, $2, $3, $4, $5, $6) ON CONFLICT (contract_address) DO NOTHING", record.ContractAddress, record.Deployer, record.Codehash, record.CreationTime, record.TxHash, record.BlockNum)
	if err != nil {
		log.Fatal().Msgf("Failed to insert a contract deploy record into the database, error: %v", err)
		conn.Release()
		return err
	}

	_, err = conn.Exec(context.Background(), "INSERT INTO contract.hash_to_bytecode (codehash, bytecode) VALUES($1, $2) ON CONFLICT (codehash) DO NOTHING", record.Codehash, record.Bytecode)
	if err != nil {
		log.Fatal().Msgf("Failed to insert a codehash record into the database, error: %v", err)
		conn.Release()
		return err
	}

	log.Info().Msg("Inserted a record into the database, contract_address: " + record.ContractAddress)
	conn.Release()
	return nil
}

func QueryCurrentStatistics(db *pgxpool.Pool) {
	conn, err := db.Acquire(context.Background())
	if err != nil {
		log.Fatal().Msgf("Failed to acquire a connection, error: %v", err)

	}

	var contractCount int
	var codehashCount int

	err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM contract.contract_deploy_record").Scan(&contractCount)
	if err != nil {
		log.Fatal().Msgf("Failed to query the contract count, error: %v", err)
		conn.Release()
	}
	log.Info().Msgf("Current contract count: %d", contractCount)

	err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM contract.hash_to_bytecode").Scan(&codehashCount)
	if err != nil {
		log.Fatal().Msgf("Failed to query the codehash count, error: %v", err)
		conn.Release()
	}

	log.Info().Msgf("Current codehash count: %d", codehashCount)

	conn.Release()
}

func LastBlockNumber(db *pgxpool.Pool) (int64, error) {
	conn, err := db.Acquire(context.Background())
	if err != nil {
		log.Fatal().Msgf("Failed to acquire a connection, error: %v", err)
		return 0, err
	}

	var blockNum int64
	err = conn.QueryRow(context.Background(), "SELECT block_num FROM contract.contract_deploy_record ORDER BY block_num DESC LIMIT 1").Scan(&blockNum)
	if err != nil {
		log.Fatal().Msgf("Failed to query the last block number, error: %v", err)
		conn.Release()
		return 0, err
	}

	conn.Release()
	return blockNum, nil
}
