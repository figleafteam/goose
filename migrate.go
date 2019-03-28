package goose

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrNoCurrentVersion when a current migration version is not found.
	ErrNoCurrentVersion = errors.New("no current version found")
	// ErrNoNextVersion when the next migration version is not found.
	ErrNoNextVersion = errors.New("no next version found")
	// MaxVersion is the maximum allowed version.
	MaxVersion int64 = 9223372036854775807 // max(int64)

	registeredGoMigrations = map[int64]*Migration{}
)

// Migrations slice.
type Migrations []*Migration

// helpers so we can use pkg sort
func (ms Migrations) Len() int      { return len(ms) }
func (ms Migrations) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
func (ms Migrations) Less(i, j int) bool {
	if ms[i].Version == ms[j].Version {
		panic(fmt.Sprintf("goose: duplicate version %v detected:\n%v\n%v", ms[i].Version, ms[i].Source, ms[j].Source))
	}
	return ms[i].Version < ms[j].Version
}

// Current gets the current migration.
func (ms Migrations) Current(current int64) (*Migration, error) {
	for i, migration := range ms {
		if migration.Version == current {
			return ms[i], nil
		}
	}

	return nil, ErrNoCurrentVersion
}

// Next gets the next migration.
func (ms Migrations) Next(current int64) (*Migration, error) {
	if current == 0 {
		return ms[0], nil
	}
	cur, err := ms.Current(current)
	if err != nil {
		return nil, err
	}
	if cur.Next == -1 {
		return nil, ErrNoNextVersion
	}
	next, err := ms.Current(cur.Next)
	if err != nil {
		return nil, err
	}

	return next, nil
}

// Previous : Get the previous migration.
func (ms Migrations) Previous(current int64) (*Migration, error) {
	cur, err := ms.Current(current)
	if err != nil {
		return nil, err
	}
	if cur.Previous == -1 {
		return nil, ErrNoNextVersion
	}
	prev, err := ms.Current(cur.Previous)
	if err != nil {
		return nil, err
	}

	return prev, nil
}

// Last gets the last migration.
func (ms Migrations) Last() (*Migration, error) {
	if len(ms) == 0 {
		return nil, ErrNoNextVersion
	}

	return ms[len(ms)-1], nil
}

// Versioned gets versioned migrations.
func (ms Migrations) versioned() (Migrations, error) {
	var migrations Migrations

	// assume that the user will never have more than 19700101000000 migrations
	for _, m := range ms {
		// parse version as timestmap
		versionTime, err := time.Parse(timestampFormat, fmt.Sprintf("%d", m.Version))

		if versionTime.Before(time.Unix(0, 0)) || err != nil {
			migrations = append(migrations, m)
		}
	}

	return migrations, nil
}

// Timestamped gets the timestamped migrations.
func (ms Migrations) timestamped() (Migrations, error) {
	var migrations Migrations

	// assume that the user will never have more than 19700101000000 migrations
	for _, m := range ms {
		// parse version as timestmap
		versionTime, err := time.Parse(timestampFormat, fmt.Sprintf("%d", m.Version))
		if err != nil {
			// probably not a timestamp
			continue
		}

		if versionTime.After(time.Unix(0, 0)) {
			migrations = append(migrations, m)
		}
	}
	return migrations, nil
}

func (ms Migrations) String() string {
	str := ""
	for _, m := range ms {
		str += fmt.Sprintln(m)
	}
	return str
}

type TimestampedMigrations Migrations

// helpers so we can use pkg sort
func (ms TimestampedMigrations) Len() int      { return len(ms) }
func (ms TimestampedMigrations) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
func (ms TimestampedMigrations) Less(i, j int) bool {
	if ms[i].Version == ms[j].Version {
		panic(fmt.Sprintf("goose: duplicate version %v detected:\n%v\n%v", ms[i].Version, ms[i].Source, ms[j].Source))
	}
	return ms[i].Version < ms[j].Version
}

// Current gets the current migration.
func (ms TimestampedMigrations) Current(current int64) (*Migration, error) {
	for i, migration := range ms {
		if migration.Version == current {
			return ms[i], nil
		}
	}

	return nil, ErrNoCurrentVersion
}

// Next gets the next migration.
func (ms TimestampedMigrations) Next(current int64) (*Migration, error) {
	cur, err := ms.Current(current)
	if err != nil {
		return nil, err
	}
	if cur.Next == -1 {
		return nil, ErrNoNextVersion
	}
	next, err := ms.Current(cur.Next)
	if err != nil {
		return nil, err
	}

	return next, nil
}

// Previous : Get the previous migration.
func (ms TimestampedMigrations) Previous(current int64) (*Migration, error) {
	cur, err := ms.Current(current)
	if err != nil {
		return nil, err
	}
	if cur.Previous == -1 {
		return nil, ErrNoNextVersion
	}
	prev, err := ms.Current(cur.Previous)
	if err != nil {
		return nil, err
	}

	return prev, nil
}

// Last gets the last migration.
func (ms TimestampedMigrations) Last() (*Migration, error) {
	if len(ms) == 0 {
		return nil, ErrNoNextVersion
	}

	return ms[len(ms)-1], nil
}

// Versioned gets versioned migrations.
func (ms TimestampedMigrations) versioned() (TimestampedMigrations, error) {
	var migrations TimestampedMigrations

	// assume that the user will never have more than 19700101000000 migrations
	for _, m := range ms {
		// parse version as timestmap
		versionTime, err := time.Parse(timestampFormat, fmt.Sprintf("%d", m.Version))

		if versionTime.Before(time.Unix(0, 0)) || err != nil {
			migrations = append(migrations, m)
		}
	}

	return migrations, nil
}

// Timestamped gets the timestamped migrations.
func (ms TimestampedMigrations) timestamped() (TimestampedMigrations, error) {
	var migrations TimestampedMigrations

	// assume that the user will never have more than 19700101000000 migrations
	for _, m := range ms {
		// parse version as timestmap
		versionTime, err := time.Parse(timestampFormat, fmt.Sprintf("%d", m.Version))
		if err != nil {
			// probably not a timestamp
			continue
		}

		if versionTime.After(time.Unix(0, 0)) {
			migrations = append(migrations, m)
		}
	}
	return migrations, nil
}

func (ms TimestampedMigrations) String() string {
	str := ""
	for _, m := range ms {
		str += fmt.Sprintln(m)
	}
	return str
}

// AddMigration adds a migration.
func AddMigration(up func(*sql.Tx) error, down func(*sql.Tx) error) {
	_, filename, _, _ := runtime.Caller(1)
	AddNamedMigration(filename, up, down)
}

// AddNamedMigration : Add a named migration.
func AddNamedMigration(filename string, up func(*sql.Tx) error, down func(*sql.Tx) error) {
	v, _ := NumericComponent(filename)
	migration := &Migration{Version: v, Next: -1, Previous: -1, Registered: true, UpFn: up, DownFn: down, Source: filename}

	if existing, ok := registeredGoMigrations[v]; ok {
		panic(fmt.Sprintf("failed to add migration %q: version conflicts with %q", filename, existing.Source))
	}

	registeredGoMigrations[v] = migration
}

// CollectMigrations returns all the valid looking migration scripts in the
// migrations folder and go func registry, and key them by version.
func CollectMigrations(dirpath string, current, target int64) (Migrations, error) {
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		return nil, fmt.Errorf("%s directory does not exists", dirpath)
	}

	var migrations Migrations

	// SQL migration files.
	sqlMigrationFiles, err := filepath.Glob(dirpath + "/**.sql")
	if err != nil {
		return nil, err
	}
	for _, file := range sqlMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			return nil, err
		}
		if versionFilter(v, current, target) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file}
			migrations = append(migrations, migration)
		}
	}

	// Go migrations registered via goose.AddMigration().
	for _, migration := range registeredGoMigrations {
		v, err := NumericComponent(migration.Source)
		if err != nil {
			return nil, err
		}
		if versionFilter(v, current, target) {
			migrations = append(migrations, migration)
		}
	}

	// Go migration files
	goMigrationFiles, err := filepath.Glob(dirpath + "/**.go")
	if err != nil {
		return nil, err
	}
	for _, file := range goMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			continue // Skip any files that don't have version prefix.
		}

		// Skip migrations already existing migrations registered via goose.AddMigration().
		if _, ok := registeredGoMigrations[v]; ok {
			continue
		}

		if versionFilter(v, current, target) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file, Registered: false}
			migrations = append(migrations, migration)
		}
	}

	migrations = sortAndConnectMigrations(migrations)

	return migrations, nil
}

// CollectAllMigrations returns all the valid looking migration scripts in the
// migrations folder and go func registry, and key them by version.
func CollectAllMigrations(dirpath string, applied map[int64]bool, current, target int64) (Migrations, error) {
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		return nil, fmt.Errorf("%s directory does not exists", dirpath)
	}

	var migrations Migrations

	// SQL migration files.
	sqlMigrationFiles, err := filepath.Glob(dirpath + "/**.sql")
	if err != nil {
		return nil, err
	}
	for _, file := range sqlMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			return nil, err
		}
		if unappliedVersionFilter(v, current, target, applied[v]) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file}
			migrations = append(migrations, migration)
		}
	}

	// Go migrations registered via goose.AddMigration().
	for _, migration := range registeredGoMigrations {
		v, err := NumericComponent(migration.Source)
		if err != nil {
			return nil, err
		}
		if unappliedVersionFilter(v, current, target, applied[v]) {
			migrations = append(migrations, migration)
		}
	}

	// Go migration files
	goMigrationFiles, err := filepath.Glob(dirpath + "/**.go")
	if err != nil {
		return nil, err
	}
	for _, file := range goMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			continue // Skip any files that don't have version prefix.
		}

		// Skip migrations already existing migrations registered via goose.AddMigration().
		if _, ok := registeredGoMigrations[v]; ok {
			continue
		}

		if unappliedVersionFilter(v, current, target, applied[v]) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file, Registered: false}
			migrations = append(migrations, migration)
		}
	}

	migrations = sortAndConnectAllMigrations(migrations, applied)

	return migrations, nil
}

func sortAndConnectAllMigrations(migrations Migrations, applied map[int64]bool) Migrations {
	sort.Sort(migrations)

	var am Migrations
	var um Migrations

	// now that we're sorted in the appropriate direction,
	// populate next and previous for each migration
	for _, m := range migrations {
		a := applied[m.Version]
		// append applied
		if a {
			am = append(am, m)
			continue
		}
		//append unapplied
		um = append(um, m)
	}
	//connect applied migrations
	for i, m := range am {
		prev := int64(-1)
		if i > 0 {
			prev = migrations[i-1].Version
			migrations[i-1].Next = m.Version
		}
		migrations[i].Previous = prev
	}
	//connect unapplied migrations
	for i, m := range um {
		prev := int64(-1)
		if i > 0 {
			prev = um[i-1].Version
			um[i-1].Next = m.Version
		}
		um[i].Previous = prev
	}

	if len(am) == 0 {
		migrations = um
		return migrations
	}

	if len(um) != 0 {
		if len(um) == 1 {
			um[0].Next = -1
		}
		am[len(am)-1].Next = um[0].Version
		um[0].Previous = am[len(am)-1].Version
	}

	migrations = append(am, um...)

	return migrations
}

func sortAndConnectMigrations(migrations Migrations) Migrations {
	sort.Sort(migrations)

	// now that we're sorted in the appropriate direction,
	// populate next and previous for each migration
	for i, m := range migrations {
		prev := int64(-1)
		if i > 0 {
			prev = migrations[i-1].Version
			migrations[i-1].Next = m.Version
		}
		migrations[i].Previous = prev
	}

	return migrations
}

func versionFilter(v, current, target int64) bool {

	if target > current {
		return v > current && v <= target
	}

	if target < current {
		return v <= current && v > target
	}

	return false
}

func unappliedVersionFilter(v, current, target int64, applied bool) bool {
	if !applied {
		return true
	}

	if target > current {
		return v > current && v <= target
	}

	return false
}

// retrieve the current version for this DB.
// Create and initialize the DB version table if it doesn't exist.
func AppliedDBVersions(db *sql.DB) (map[int64]bool, error) {

	applied := make(map[int64]bool)

	rows, err := GetDialect().dbVersionQuery(db)
	if err != nil {
		return applied, createVersionTable(db)
	}
	defer rows.Close()

	failed := make(map[int64]bool)

	for rows.Next() {
		var row MigrationRecord
		if err = rows.Scan(&row.ID, &row.VersionID, &row.IsApplied, &row.TStamp); err != nil {
			log.Fatal("error scanning rows:", err)
		}

		// Mark a migration as applied, only if the latest occurrence of it is
		// with truthy is_applied column. Expect version sorted in descending
		// order for this whole scheme to work.
		if row.IsApplied && !failed[row.VersionID] {
			applied[row.VersionID] = true
		} else {
			failed[row.VersionID] = true
		}
	}

	return applied, nil
}

// EnsureDBVersion retrieves the current version for this DB.
// Create and initialize the DB version table if it doesn't exist.
func EnsureDBVersion(db *sql.DB) (int64, error) {
	rows, err := GetDialect().dbVersionQuery(db)
	if err != nil {
		return 0, createVersionTable(db)
	}
	defer rows.Close()

	// The most recent record for each migration specifies
	// whether it has been applied or rolled back.
	// The first version we find that has been applied is the current version.

	toSkip := make([]int64, 0)

	for rows.Next() {
		var row MigrationRecord
		if err = rows.Scan(&row.ID, &row.VersionID, &row.IsApplied, &row.TStamp); err != nil {
			return 0, errors.Wrap(err, "failed to scan row")
		}

		// have we already marked this version to be skipped?
		skip := false
		for _, v := range toSkip {
			if v == row.VersionID {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		// if version has been applied we're done
		if row.IsApplied {
			return row.VersionID, nil
		}

		// latest version of migration has not been applied.
		toSkip = append(toSkip, row.VersionID)
	}
	if err := rows.Err(); err != nil {
		return 0, errors.Wrap(err, "failed to get next row")
	}

	return 0, ErrNoNextVersion
}

// Create the db version table
// and insert the initial 0 value into it
func createVersionTable(db *sql.DB) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	d := GetDialect()

	if _, err := txn.Exec(d.createVersionTableSQL()); err != nil {
		txn.Rollback()
		return err
	}

	version := 0
	applied := true
	if _, err := txn.Exec(d.insertVersionSQL(), version, applied); err != nil {
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

// GetDBVersion is an alias for EnsureDBVersion, but returns -1 in error.
func GetDBVersion(db *sql.DB) (int64, error) {
	version, err := EnsureDBVersion(db)
	if err != nil {
		return -1, err
	}

	return version, nil
}
