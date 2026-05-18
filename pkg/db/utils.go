package db

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver
	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose/v3"
)

// PostgresSQLX returns a sqlx connection. Usage: https://jmoiron.github.io/sqlx/
func PostgresSQLX(ds Datasource) (*sqlx.DB, error) {
	sources := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", ds.User, ds.Pass, ds.Host, ds.Port, ds.Name)
	db, err := sqlx.Connect("pgx", sources)
	if err != nil {
		return nil, fmt.Errorf("utils: failed to connect to db (%s) - %w", ds.Name, err)
	}

	_, err = db.Exec("SET TIME ZONE 'UTC'")
	if err != nil {
		return nil, fmt.Errorf("utils: failed to set time zone - %w", err)
	}

	_, err = db.Exec(fmt.Sprintf("SET search_path TO %s", ds.Schema))
	if err != nil {
		return nil, fmt.Errorf("utils: failed to set search path - %w", err)
	}

	return db, nil
}

func Migrate(db *sql.DB, migrations embed.FS, appName, migrationsDir string) error {
	slog.Info("utils: start applying migrations", "embed", migrations)

	goose.SetBaseFS(migrations)
	appName = strings.ToLower(strings.ReplaceAll(appName, "-", "_"))
	goose.SetTableName("migrations_" + appName)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("utils: failed to set goose dialect - %w", err)
	}

	if migrationsDir == "" {
		migrationsDir = "."
	}

	if err := goose.Up(db, migrationsDir); err != nil {
		if errors.Is(err, goose.ErrNoNextVersion) {
			slog.Info("utils: no new migrations to apply")
			return nil
		}
		return fmt.Errorf("utils: failed to apply migrations - %w", err)
	}

	return nil
}
