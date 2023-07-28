package oracle

import (
	"fmt"
	"gorm.io/gorm/schema"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
}

func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw(
		fmt.Sprintf(`SELECT ORA_DATABASE_NAME as "Current Database" FROM %s`, m.Dialector.(Dialector).DummyTableName()),
	).Row().Scan(&name)
	return
}

func (m Migrator) CreateTable(values ...interface{}) error {
	for _, value := range values {
		m.TryQuotifyReservedWords(value)
		m.TryRemoveOnUpdate(value)
	}
	return m.Migrator.CreateTable(values...)
}

func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	for i := len(values) - 1; i >= 0; i-- {
		value := values[i]
		tx := m.DB.Session(&gorm.Session{})
		if m.HasTable(value) {
			if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
				return tx.Exec("DROP TABLE ? CASCADE CONSTRAINTS", clause.Table{Name: stmt.Table}).Error
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int64

	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw("SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = ?", stmt.Table).Row().Scan(&count)
	})

	return count > 0
}

func (m Migrator) RenameTable(oldName, newName interface{}) (err error) {
	resolveTable := func(name interface{}) (result string, err error) {
		if v, ok := name.(string); ok {
			result = v
		} else {
			stmt := &gorm.Statement{DB: m.DB}
			if err = stmt.Parse(name); err == nil {
				result = stmt.Table
			}
		}
		return
	}

	var oldTable, newTable string

	if oldTable, err = resolveTable(oldName); err != nil {
		return
	}

	if newTable, err = resolveTable(newName); err != nil {
		return
	}

	if !m.HasTable(oldTable) {
		return
	}

	return m.DB.Exec("RENAME TABLE ? TO ?",
		clause.Table{Name: oldTable},
		clause.Table{Name: newTable},
	).Error
}

func (m Migrator) CreateConstraint(value interface{}, name string) error {
	m.TryRemoveOnUpdate(value)
	return m.Migrator.CreateConstraint(value, name)
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		for _, chk := range stmt.Schema.ParseCheckConstraints() {
			if chk.Name == name {
				return m.DB.Exec(
					"ALTER TABLE ? DROP CHECK ?",
					clause.Table{Name: stmt.Table}, clause.Column{Name: name},
				).Error
			}
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP CONSTRAINT ?",
			clause.Table{Name: stmt.Table}, clause.Column{Name: name},
		).Error
	})
}

func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM USER_CONSTRAINTS WHERE TABLE_NAME = ? AND CONSTRAINT_NAME = ?", stmt.Table, name,
		).Row().Scan(&count)
	}) == nil && count > 0
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}, clause.Table{Name: stmt.Table}).Error
	})
}

func (m Migrator) HasIndex(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Raw(
			"SELECT COUNT(*) FROM USER_INDEXES WHERE TABLE_NAME = ? AND INDEX_NAME = ?",
			m.Migrator.DB.NamingStrategy.TableName(stmt.Table),
			m.Migrator.DB.NamingStrategy.IndexName(stmt.Table, name),
		).Row().Scan(&count)
	})

	return count > 0
}

// https://docs.oracle.com/database/121/SPATL/alter-index-rename.htm
func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	panic("TODO")
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec(
			"ALTER INDEX ?.? RENAME TO ?", // wat
			clause.Table{Name: stmt.Table}, clause.Column{Name: oldName}, clause.Column{Name: newName},
		).Error
	})
}

func (m Migrator) TryRemoveOnUpdate(values ...interface{}) error {
	for _, value := range values {
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			for _, rel := range stmt.Schema.Relationships.Relations {
				constraint := rel.ParseConstraint()
				if constraint != nil {
					rel.Field.TagSettings["CONSTRAINT"] = strings.ReplaceAll(rel.Field.TagSettings["CONSTRAINT"], fmt.Sprintf("ON UPDATE %s", constraint.OnUpdate), "")
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) TryQuotifyReservedWords(values ...interface{}) error {
	for _, value := range values {
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			for idx, v := range stmt.Schema.DBNames {
				if IsReservedWord(v) {
					stmt.Schema.DBNames[idx] = fmt.Sprintf(`"%s"`, v)
				}
			}

			for _, v := range stmt.Schema.Fields {
				if IsReservedWord(v.DBName) {
					v.DBName = fmt.Sprintf(`"%s"`, v.DBName)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (interface{}, interface{}) {
	if strings.Contains(table, ".") {
		if tables := strings.Split(table, `.`); len(tables) == 2 {
			return tables[0], tables[1]
		}
	}

	if stmt.TableExpr != nil {
		if tables := strings.Split(stmt.TableExpr.SQL, `"."`); len(tables) == 2 {
			return strings.TrimPrefix(tables[0], `"`), table
		}
	}
	return clause.Expr{SQL: "sys_context( 'userenv', 'current_schema' )"}, table
}

func (m Migrator) CreateSequence(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field,
	serialDatabaseType string) (err error) {

	_, table := m.CurrentSchema(stmt, stmt.Table)
	tableName := table.(string)

	sequenceName := strings.Join([]string{tableName, field.DBName, "seq"}, "_")
	if err = tx.Exec(`CREATE SEQUENCE IF NOT EXISTS ? AS ?`, clause.Expr{SQL: sequenceName},
		clause.Expr{SQL: serialDatabaseType}).Error; err != nil {
		return err
	}

	if err := tx.Exec("ALTER TABLE ? ALTER COLUMN ? SET DEFAULT nextval('?')",
		clause.Expr{SQL: tableName}, clause.Expr{SQL: field.DBName}, clause.Expr{SQL: sequenceName}).Error; err != nil {
		return err
	}

	if err := tx.Exec("ALTER SEQUENCE ? OWNED BY ?.?",
		clause.Expr{SQL: sequenceName}, clause.Expr{SQL: tableName}, clause.Expr{SQL: field.DBName}).Error; err != nil {
		return err
	}
	return
}

func (m Migrator) UpdateSequence(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field,
	serialDatabaseType string) (err error) {

	sequenceName, err := m.getColumnSequenceName(tx, stmt, field)
	if err != nil {
		return err
	}

	if err = tx.Exec(`ALTER SEQUENCE IF EXISTS ? AS ?`, clause.Expr{SQL: sequenceName}, clause.Expr{SQL: serialDatabaseType}).Error; err != nil {
		return err
	}

	if err := tx.Exec("ALTER TABLE ? ALTER COLUMN ? TYPE ?",
		m.CurrentTable(stmt), clause.Expr{SQL: field.DBName}, clause.Expr{SQL: serialDatabaseType}).Error; err != nil {
		return err
	}
	return
}

func (m Migrator) DeleteSequence(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field,
	fileType clause.Expr) (err error) {

	sequenceName, err := m.getColumnSequenceName(tx, stmt, field)
	if err != nil {
		return err
	}

	if err := tx.Exec("ALTER TABLE ? ALTER COLUMN ? TYPE ?", m.CurrentTable(stmt), clause.Column{Name: field.DBName}, fileType).Error; err != nil {
		return err
	}

	if err := tx.Exec("ALTER TABLE ? ALTER COLUMN ? DROP DEFAULT",
		m.CurrentTable(stmt), clause.Expr{SQL: field.DBName}).Error; err != nil {
		return err
	}

	if err = tx.Exec(`DROP SEQUENCE IF EXISTS ?`, clause.Expr{SQL: sequenceName}).Error; err != nil {
		return err
	}

	return
}

func (m Migrator) getColumnSequenceName(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field) (
	sequenceName string, err error) {
	_, table := m.CurrentSchema(stmt, stmt.Table)

	// DefaultValueValue is reset by ColumnTypes, search again.
	var columnDefault string
	err = tx.Raw(
		`SELECT column_default FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
		table, field.DBName).Scan(&columnDefault).Error

	if err != nil {
		return
	}

	sequenceName = strings.TrimSuffix(
		strings.TrimPrefix(columnDefault, `nextval('`),
		`'::regclass)`,
	)
	return
}

func getSerialDatabaseType(s string) (dbType string, ok bool) {
	switch s {
	case "smallserial":
		return "integer", true
	case "serial":
		return "integer", true
	case "bigserial":
		return "number", true
	default:
		return "", false
	}
}

func (m Migrator) resetPreparedStmts() {
	if m.DB.PrepareStmt {
		if pdb, ok := m.DB.ConnPool.(*gorm.PreparedStmtDB); ok {
			pdb.Reset()
		}
	}
}

//func (m Migrator) GetRows(currentSchema interface{}, table interface{}) (*sql.Rows, error) {
//	name := table.(string)
//	if _, ok := currentSchema.(string); ok {
//		name = fmt.Sprintf("%v.%v", currentSchema, table)
//	}
//
//	return m.DB.Session(&gorm.Session{}).Table(name).Limit(1).Scopes(func(d *gorm.DB) *gorm.DB {
//		//dialector, _ := m.Dialector.(Dialector)
//		// use simple protocol
//		//if !m.DB.PrepareStmt && (dialector.Config != nil && (dialector.Config.DriverName == "" || dialector.Config.DriverName == "godror")) {
//		//	d.Statement.Vars = append([]interface{}{godror.QueryExecModeSimpleProtocol}, d.Statement.Vars...)
//		//}
//		return d
//	}).Rows()
//}
