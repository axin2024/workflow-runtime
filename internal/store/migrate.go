package store

import (
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	mysqlmigrate "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

const SchemaVersion = 4

//go:embed migrations/*.sql
var migrations embed.FS

func Migrate(dsn string) (uint, error) {
	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		return 0, fmt.Errorf("open migrations: %w", err)
	}
	defer source.Close()
	db, err := openDB(dsn, true)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	driver, err := mysqlmigrate.WithInstance(db, &mysqlmigrate.Config{StatementTimeout: 30 * time.Second})
	if err != nil {
		return 0, fmt.Errorf("prepare migration database: %w", err)
	}
	migration, err := migrate.NewWithInstance("iofs", source, "mysql", driver)
	if err != nil {
		_ = driver.Close()
		return 0, fmt.Errorf("prepare migrations: %w", err)
	}
	defer migration.Close()
	if err := migration.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return 0, fmt.Errorf("apply migrations: %w", err)
	}
	version, dirty, err := migration.Version()
	if err != nil {
		return 0, fmt.Errorf("verify migrations: %w", err)
	}
	if version != SchemaVersion || dirty {
		return 0, fmt.Errorf("expected clean migration version %d", SchemaVersion)
	}
	return version, nil
}
