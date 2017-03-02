package isql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/stretchr/testify/mock"
	"reflect"
	"time"
)

type MockOpener struct {
	mock.Mock
}

func (m *MockOpener) Open(driverName, dataSourceName string) (DB, error) {
	args := m.Called(driverName, dataSourceName)
	db := args.Get(0)
	if db == nil {
		return nil, args.Error(1)
	}
	return db.(DB), args.Error(1)
}

type MockDB struct {
	mock.Mock
}

func (m *MockDB) Begin() (Tx, error) {
	res := m.Called()
	return unpackTx(res.Get(0)), res.Error(1)
}

func (m *MockDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	res := m.Called(ctx, opts)
	return unpackTx(res.Get(0)), res.Error(1)
}

func (m *MockDB) Close() error {
	res := m.Called()
	return res.Error(0)
}

func (m *MockDB) Driver() driver.Driver {
	res := m.Called()
	d := res.Get(0)
	if d == nil {
		return nil
	}
	return d.(driver.Driver)
}

func (m *MockDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackSqlResult(res.Get(0)), res.Error(1)
}

func (m *MockDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	params := make([]interface{}, 0, len(args)+2)
	params = append(params, ctx)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackSqlResult(res.Get(0)), res.Error(1)
}

func (m *MockDB) Ping() error {
	return m.Called().Error(0)
}

func (m *MockDB) PingContext(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}

func (m *MockDB) Prepare(query string) (Stmt, error) {
	res := m.Called(query)
	return unpackStmt(res.Get(0)), res.Error(1)
}

func (m *MockDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	res := m.Called(ctx, query)
	return unpackStmt(res.Get(0)), res.Error(1)
}

func (m *MockDB) Query(query string, args ...interface{}) (Rows, error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRows(res.Get(0)), res.Error(1)
}

func (m *MockDB) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	params := make([]interface{}, 0, len(args)+2)
	params = append(params, ctx)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRows(res.Get(0)), res.Error(1)
}

func (m *MockDB) QueryRow(query string, args ...interface{}) Row {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRow(res.Get(0))
}

func (m *MockDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	params := make([]interface{}, 0, len(args)+2)
	params = append(params, ctx)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRow(res.Get(0))
}

func (m *MockDB) SetConnMaxLifetime(dur time.Duration) {
	m.Called(dur)
}

func (m *MockDB) SetMaxIdleConns(n int) {
	m.Called(n)
}

func (m *MockDB) SetMaxOpenConns(n int) {
	m.Called(n)
}

func (m *MockDB) Stats() sql.DBStats {
	return m.Called(0).Get(0).(sql.DBStats)
}

func unpackRow(i interface{}) Row {
	if i == nil {
		return nil
	}
	return i.(Row)
}

type MockRow struct {
	mock.Mock
}

func (m *MockRow) Scan(dest ...interface{}) error {
	return m.Called(dest...).Error(0)
}

func unpackRows(i interface{}) Rows {
	if i == nil {
		return nil
	}
	return i.(Rows)
}

type MockRows struct {
	mock.Mock
}

func (m *MockRows) Close() error {
	return m.Called().Error(0)
}

func (m *MockRows) ColumnTypes() ([]ColumnType, error) {
	res := m.Called()
	columnTypes := res.Get(0)
	if columnTypes == nil {
		return nil, res.Error(1)
	}
	return columnTypes.([]ColumnType), res.Error(1)
}

func (m *MockRows) Columns() ([]string, error) {
	res := m.Called()
	columns := res.Get(0)
	if columns == nil {
		return nil, res.Error(1)
	}
	return columns.([]string), res.Error(1)
}

func (m *MockRows) Err() error {
	return m.Called().Error(0)
}

func (m *MockRows) Next() bool {
	return m.Called().Bool(0)
}

func (m *MockRows) NextResultSet() bool {
	return m.Called().Bool(0)
}

func (m *MockRows) Scan(dest ...interface{}) error {
	return m.Called(dest...).Error(0)
}

func unpackStmt(i interface{}) Stmt {
	if i == nil {
		return nil
	}
	return i.(Stmt)
}

type MockStmt struct {
	mock.Mock
}

func (m *MockStmt) Close() error {
	return m.Called().Error(0)
}

func (m *MockStmt) Exec(args ...interface{}) (sql.Result, error) {
	res := m.Called(args...)
	return unpackSqlResult(res.Get(0)), res.Error(1)
}

func (m *MockStmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, ctx)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackSqlResult(res.Get(0)), res.Error(1)
}

func (m *MockStmt) Query(args ...interface{}) (Rows, error) {
	res := m.Called(args...)
	return unpackRows(res.Get(0)), res.Error(1)
}

func (m *MockStmt) QueryContext(ctx context.Context, args ...interface{}) (Rows, error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, ctx)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRows(res.Get(0)), res.Error(1)
}

func (m *MockStmt) QueryRow(args ...interface{}) Row {
	res := m.Called(args...)
	return unpackRow(res.Get(0))
}

func (m *MockStmt) QueryRowContext(ctx context.Context, args ...interface{}) Row {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, ctx)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRow(res.Get(0))
}

func unpackTx(i interface{}) Tx {
	if i == nil {
		return nil
	}
	return i.(Tx)
}

type MockTx struct {
	mock.Mock
}

func (m *MockTx) Commit() error {
	return m.Called().Error(0)
}

func (m *MockTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackSqlResult(res.Get(0)), res.Error(1)
}

func (m *MockTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	params := make([]interface{}, 0, len(args)+2)
	params = append(params, ctx)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackSqlResult(res.Get(0)), res.Error(1)
}

func (m *MockTx) Prepare(query string) (Stmt, error) {
	res := m.Called(query)
	return unpackStmt(res.Get(0)), res.Error(1)
}

func (m *MockTx) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	res := m.Called(ctx, query)
	return unpackStmt(res.Get(0)), res.Error(1)
}

func (m *MockTx) Query(query string, args ...interface{}) (Rows, error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRows(res.Get(0)), res.Error(1)
}

func (m *MockTx) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	params := make([]interface{}, 0, len(args)+2)
	params = append(params, ctx)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRows(res.Get(0)), res.Error(1)
}

func (m *MockTx) QueryRow(query string, args ...interface{}) Row {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRow(res.Get(0))
}

func (m *MockTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	params := make([]interface{}, 0, len(args)+2)
	params = append(params, ctx)
	params = append(params, query)
	params = append(params, args...)
	res := m.Called(params...)
	return unpackRow(res.Get(0))
}

func (m *MockTx) Rollback() error {
	return m.Called().Error(0)
}

func (m *MockTx) Stmt(stmt *sql.Stmt) Stmt {
	return unpackStmt(m.Called(stmt).Get(0))
}

func (m *MockTx) StmtContext(ctx context.Context, stmt *sql.Stmt) Stmt {
	return unpackStmt(m.Called(ctx, stmt).Get(0))
}

type MockColumnType struct {
	mock.Mock
}

func (m *MockColumnType) DatabaseTypeName() string {
	return m.Called().String(0)
}

func (m *MockColumnType) DecimalSize() (precision, scale int64, ok bool) {
	res := m.Called()
	return res.Get(0).(int64), res.Get(1).(int64), res.Bool(2)
}

func (m *MockColumnType) Length() (length int64, ok bool) {
	res := m.Called()
	return res.Get(0).(int64), res.Bool(1)
}

func (m *MockColumnType) Name() string {
	return m.Called().String(0)
}

func (m *MockColumnType) Nullable() (nullable, ok bool) {
	res := m.Called()
	return res.Bool(0), res.Bool(1)
}

func (m *MockColumnType) ScanType() reflect.Type {
	res := m.Called()
	i := res.Get(0)
	if i == nil {
		return nil
	}
	return i.(reflect.Type)
}

func unpackSqlResult(i interface{}) sql.Result {
	if i == nil {
		return nil
	}
	return i.(sql.Result)
}

type MockResult struct {
	mock.Mock
}

func (m *MockResult) LastInsertId() (int64, error) {
	res := m.Called()
	return res.Get(0).(int64), res.Error(1)
}

func (m *MockResult) RowsAffected() (int64, error) {
	res := m.Called()
	return res.Get(0).(int64), res.Error(1)
}
