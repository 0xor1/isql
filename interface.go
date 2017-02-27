package isql

import (
	"database/sql"
	"database/sql/driver"
	"time"
	"context"
	"reflect"
)

func NewDb(db *sql.DB) DB {
	if db == nil {
		return nil
	}
	return &dbWrapper{
		db: db,
	}
}

type DB interface {
	Begin() (Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	Close() error
	Driver() driver.Driver
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...interface{}) (Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error)
	QueryRow(query string, args ...interface{}) Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) Row
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
}

func NewRow(row *sql.Row) Row {
	if row == nil {
		return nil
	}
	return &rowWrapper{
		row: row,
	}
}

type Row interface{
	Scan(dest ...interface{}) error
}

func NewRows(rows *sql.Rows) Rows {
	if rows == nil {
		return nil
	}
	return &rowsWrapper{
		rows: rows,
	}
}

type Rows interface{
	Close() error
	ColumnTypes() ([]ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(dest ...interface{}) error
}

func NewStmt(stmt *sql.Stmt) Stmt {
	if stmt == nil {
		return nil
	}
	return &stmtWrapper{
		stmt: stmt,
	}
}

type Stmt interface{
	Close() error
	Exec(args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
	Query(args ...interface{}) (Rows, error)
	QueryContext(ctx context.Context, args ...interface{}) (Rows, error)
	QueryRow(args ...interface{}) Row
	QueryRowContext(ctx context.Context, args ...interface{}) Row
}

func NewTx(tx *sql.Tx) Tx {
	if tx == nil {
		return nil
	}
	return &txWrapper{
		tx: tx,
	}
}

type Tx interface{
	Commit() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...interface{}) (Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error)
	QueryRow(query string, args ...interface{}) Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) Row
	Rollback() error
	Stmt(stmt *sql.Stmt) Stmt
	StmtContext(ctx context.Context, stmt *sql.Stmt) Stmt
}

func NewColumnType(columnType *sql.ColumnType) ColumnType {
	if columnType == nil {
		return nil
	}
	return &columnTypeWrapper{
		columnType: columnType,
	}
}

type ColumnType interface{
	DatabaseTypeName() string
	DecimalSize() (precision, scale int64, ok bool)
	Length() (length int64, ok bool)
	Name() string
	Nullable() (nullable, ok bool)
	ScanType() reflect.Type
}