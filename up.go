package goose

import (
	"database/sql"
	"fmt"
	"time"
)

// UpTo migrates up to a specific version.
func UpTo(db *sql.DB, dir string, version int64) error {
	migrations, err := CollectMigrations(dir, minVersion, version)
	if err != nil {
		return err
	}

	for {
		current, err := GetDBVersion(db)
		if err != nil {
			return err
		}

		next, err := migrations.Next(current)
		if err != nil {
			if err == ErrNoNextVersion {
				log.Printf("goose: no migrations to run. current version: %d\n", current)
				return nil
			}
			return err
		}

		if err = next.Up(db); err != nil {
			return err
		}
	}
}

// Up applies all available migrations.
func Up(db *sql.DB, dir string) error {
	return UpTo(db, dir, maxVersion)
}

func UpAll(db *sql.DB, dir string) error {
	applied, err := AppliedDBVersions(db)
	if err != nil {
		return err
	}

	migrations, err := CollectAllMigrations(dir, applied, minVersion, MaxVersion)
	if err != nil {
		return err
	}

	for {
		current, err := GetDBVersion(db)
		if err != nil {
			return err
		}

		next, err := migrations.Next(current)
		if err != nil {
			if err == ErrNoNextVersion {
				log.Printf("goose: no migrations to run. current version: %d\n", current)
				return fixUp(db)
			}
			return err
		}

		if err = next.Up(db); err != nil {
			return err
		}
	}
}

func fixUp(db *sql.DB) error {
	rows, err := GetDialect().dbVersionQuery(db)
	if err != nil {
		return err
	}
	defer rows.Close()
	tx, err := db.Begin()
	var prevRow *MigrationRecord
	for rows.Next() {
		row := new(MigrationRecord)
		if err = rows.Scan(&row.ID, &row.VersionID, &row.IsApplied, &row.TStamp); err != nil {
			log.Fatal("error scanning rows:", err)
		}
		if prevRow == nil {
			prevRow = row
			continue
		}
		if prevRow.ID > row.ID && prevRow.VersionID < row.VersionID {
			if err := swapRows(tx, prevRow, row); err != nil {
				_ = tx.Rollback()
				return err
			}
			continue
		} else if prevRow.ID < row.ID && prevRow.VersionID > row.VersionID {
			if err := swapRows(tx, prevRow, row); err != nil {
				_ = tx.Rollback()
				return err
			}
			prevRow = row
		}
		prevRow = row
	}

	return tx.Commit()
}

func swapRows(tx *sql.Tx, row1 *MigrationRecord, row2 *MigrationRecord) error {
	row2.ID, row1.ID = row1.ID, row2.ID
	q := fmt.Sprintf(`UPDATE "%s" SET version_id = %d, is_applied = %t, tstamp = '%s' WHERE id = %d;`, TableName(), row1.VersionID, row1.IsApplied, row1.TStamp.Format(time.RFC3339), row1.ID)
	fmt.Println(q)
	_, err := tx.Exec(q)
	if err != nil {
		return err
	}
	q = fmt.Sprintf(`UPDATE "%s" SET version_id = %d, is_applied = %t, tstamp = '%s' WHERE id = %d;`, TableName(), row2.VersionID, row2.IsApplied, row2.TStamp.Format(time.RFC3339), row2.ID)
	fmt.Println(q)
	_, err = tx.Exec(q)
	if err != nil {
		return err
	}

	return nil
}

// UpByOne migrates up by a single version.
func UpByOne(db *sql.DB, dir string) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	currentVersion, err := GetDBVersion(db)
	if err != nil {
		return err
	}

	next, err := migrations.Next(currentVersion)
	if err != nil {
		if err == ErrNoNextVersion {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
		}
		return err
	}

	if err = next.Up(db); err != nil {
		return err
	}

	return nil
}
