package oracle

import (
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
	"regexp"
	"strings"
)

func (m Migrator) MigrateColumn(dst interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	fmt.Println(field.DataType, columnType.Name())
	s, _ := columnType.ColumnType()
	fmt.Println(s)
	return nil
}

func (m Migrator) RenameColumn(dst interface{}, oldName, field string) error {
	panic("not implemented")
}

func (m Migrator) AddColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? ADD ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, m.DB.Migrator().FullDataTypeOf(field),
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) DropColumn(value interface{}, name string) error {
	if !m.HasColumn(value, name) {
		return nil
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(name); field != nil {
			name = field.DBName
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP ?",
			clause.Table{Name: stmt.Table},
			clause.Column{Name: name},
		).Error
	})
}

func (m Migrator) AlterColumn(value interface{}, field string) error {
	if !m.HasColumn(value, field) {
		return nil
	}

	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			var (
				columnTypes, _  = m.DB.Migrator().ColumnTypes(value)
				fieldColumnType *migrator.ColumnType
			)
			for _, columnType := range columnTypes {
				if columnType.Name() == field.DBName {
					fieldColumnType, _ = columnType.(*migrator.ColumnType)
				}
			}

			fileType := clause.Expr{SQL: m.DataTypeOf(field)}

			isSameType := true
			if fieldColumnType.DatabaseTypeName() != fileType.SQL {
				isSameType = false
				aliases := m.GetTypeAliases(fieldColumnType.DatabaseTypeName())
				for _, alias := range aliases {
					if strings.HasPrefix(fileType.SQL, alias) {
						isSameType = true
						break
					}
				}
			}
			if !isSameType {
				filedColumnAutoIncrement, _ := fieldColumnType.AutoIncrement()
				if field.AutoIncrement && filedColumnAutoIncrement { // update
					serialDatabaseType, _ := getSerialDatabaseType(fileType.SQL)
					if t, _ := fieldColumnType.ColumnType(); t != serialDatabaseType {
						if err := m.UpdateSequence(m.DB, stmt, field, serialDatabaseType); err != nil {
							return err
						}
					}
				} else if field.AutoIncrement && !filedColumnAutoIncrement { // create
					serialDatabaseType, _ := getSerialDatabaseType(fileType.SQL)
					if err := m.CreateSequence(m.DB, stmt, field, serialDatabaseType); err != nil {
						return err
					}
				} else if !field.AutoIncrement && filedColumnAutoIncrement { // delete
					if err := m.DeleteSequence(m.DB, stmt, field, fileType); err != nil {
						return err
					}
				} else {
					if err := m.modifyColumn(stmt, field, fileType, fieldColumnType); err != nil {
						return err
					}
				}
			}
			if uniq, _ := fieldColumnType.Unique(); !uniq && field.Unique {
				idxName := clause.Column{Name: m.DB.Config.NamingStrategy.IndexName(stmt.Table, field.DBName)}
				// Not a unique constraint but a unique index
				if !m.HasIndex(stmt.Table, idxName.Name) {
					if err := m.DB.Exec("ALTER TABLE ? ADD CONSTRAINT ? UNIQUE(?) ENABLE", m.CurrentTable(stmt), idxName, clause.Column{Name: field.DBName}).Error; err != nil {
						return err
					}
				}
			}
			if v, ok := fieldColumnType.DefaultValue(); (field.DefaultValueInterface == nil && ok) || v != field.DefaultValue {
				if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
					if field.DefaultValueInterface != nil {
						defaultStmt := &gorm.Statement{Vars: []interface{}{field.DefaultValueInterface}}
						m.Dialector.BindVarTo(defaultStmt, defaultStmt, field.DefaultValueInterface)
						if err := m.DB.Exec("ALTER TABLE ? MODIFY (? DEFAULT ?)", m.CurrentTable(stmt), clause.Column{Name: field.DBName}, clause.Expr{SQL: m.Dialector.Explain(defaultStmt.SQL.String(), field.DefaultValueInterface)}).Error; err != nil {
							return err
						}
					} else if field.DefaultValue != "(-)" {
						if err := m.DB.Exec("ALTER TABLE ? MODIFY (? DEFAULT ?)", m.CurrentTable(stmt), clause.Column{Name: field.DBName}, clause.Expr{SQL: field.DefaultValue}).Error; err != nil {
							return err
						}
					} else {
						if err := m.DB.Exec("ALTER TABLE ? MODIFY (? DEFAULT NULL)", m.CurrentTable(stmt), clause.Column{Name: field.DBName}, clause.Expr{SQL: field.DefaultValue}).Error; err != nil {
							return err
						}
					}
				}
			}
			return nil

		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})

	if err != nil {
		return err
	}
	m.resetPreparedStmts()
	return nil
}

func (m Migrator) modifyColumn(stmt *gorm.Statement, field *schema.Field, targetType clause.Expr, existingColumn *migrator.ColumnType) error {
	return m.DB.Exec(
		"ALTER TABLE ? MODIFY ? ?",
		clause.Table{Name: stmt.Table},
		clause.Column{Name: field.DBName},
		m.FullDataTypeOf(field),
	).Error
}

func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw("SELECT COUNT(*) FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?", stmt.Table, field).Row().Scan(&count)
	}) == nil && count > 0
}

func (m Migrator) ColumnTypes(value interface{}) (columnTypes []gorm.ColumnType, err error) {
	columnTypes = make([]gorm.ColumnType, 0)
	err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var (
			//currentDatabase      = m.DB.Migrator().CurrentDatabase()
			currentSchema, table = m.CurrentSchema(stmt, stmt.Table)
			columns, err         = m.DB.Raw(
				//"SELECT c.column_name, c.is_nullable = 'YES', c.udt_name, c.character_maximum_length, c.numeric_precision, c.numeric_precision_radix, c.numeric_scale, c.datetime_precision, 8 * typlen, c.column_default, pd.description, c.identity_increment FROM information_schema.columns AS c JOIN pg_type AS pgt ON c.udt_name = pgt.typname LEFT JOIN pg_catalog.pg_description as pd ON pd.objsubid = c.ordinal_position AND pd.objoid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = c.table_name AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = c.table_schema)) where table_catalog = ? AND table_schema = ? AND table_name = ?",
				"SELECT COLUMN_NAME,NULLABLE,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,DATA_DEFAULT FROM ALL_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ?",
				currentSchema, table).Rows()
		)

		if err != nil {
			return err
		}

		for columns.Next() {
			var (
				column = &migrator.ColumnType{
					PrimaryKeyValue: sql.NullBool{Valid: true},
					UniqueValue:     sql.NullBool{Valid: true},
				}
				typeLenValue      sql.NullInt64
				identityIncrement sql.NullString
				nullable          sql.NullString
			)

			err = columns.Scan(
				&column.NameValue, &nullable, &column.DataTypeValue, &column.LengthValue, &column.DecimalSizeValue,
				&column.ScaleValue, &column.DefaultValueValue,
			)
			if err != nil {
				return err
			}

			if nullable.String == "Y" {
				column.NullableValue.Bool = true
			}

			if typeLenValue.Valid && typeLenValue.Int64 > 0 {
				column.LengthValue = typeLenValue
			}

			if (strings.HasPrefix(column.DefaultValueValue.String, "nextval('") &&
				strings.HasSuffix(column.DefaultValueValue.String, "seq'::regclass)")) || (identityIncrement.Valid && identityIncrement.String != "") {
				column.AutoIncrementValue = sql.NullBool{Bool: true, Valid: true}
				column.DefaultValueValue = sql.NullString{}
			}

			if column.DefaultValueValue.Valid {
				column.DefaultValueValue.String = regexp.MustCompile(`'?(.*)\b'?:+[\w\s]+$`).ReplaceAllString(column.DefaultValueValue.String, "$1")
			}

			columnTypes = append(columnTypes, column)
		}
		columns.Close()

		// check primary, unique field
		{
			//columnTypeRows, err := m.DB.Raw("SELECT constraint_name FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE') AND c.table_catalog = ? AND c.table_schema = ? AND c.table_name = ? AND constraint_type = ?", currentDatabase, currentSchema, table, "UNIQUE").Rows()
			//if err != nil {
			//	return err
			//}
			//uniqueContraints := map[string]int{}
			//for columnTypeRows.Next() {
			//	var constraintName string
			//	columnTypeRows.Scan(&constraintName)
			//	uniqueContraints[constraintName]++
			//}
			//columnTypeRows.Close()

			columnTypeRows, err := m.DB.Raw(
				"SELECT column_name,constraint_name,constraint_type from user_constraints natural join user_cons_columns where owner=? and table_name = ?",
				currentSchema, table).Rows()
			if err != nil {
				return err
			}
			for columnTypeRows.Next() {
				var name, constraintName, columnType string
				columnTypeRows.Scan(&name, &constraintName, &columnType)
				for _, c := range columnTypes {
					mc := c.(*migrator.ColumnType)
					if mc.NameValue.String == name {
						switch columnType {
						case "P":
							mc.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
						case "U":
							//if uniqueContraints[constraintName] == 1 {
							mc.UniqueValue = sql.NullBool{Bool: true, Valid: true}
							//}
						}
						break
					}
				}
			}
			columnTypeRows.Close()
		}

		// check column type
		{
			dataTypeRows, err := m.DB.Raw(
				"SELECT COLUMN_NAME,DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ?",
				currentSchema, table).Rows()
			if err != nil {
				return err
			}

			for dataTypeRows.Next() {
				var name, dataType string
				dataTypeRows.Scan(&name, &dataType)
				for _, c := range columnTypes {
					mc := c.(*migrator.ColumnType)
					if mc.NameValue.String == name {
						mc.ColumnTypeValue = sql.NullString{String: dataType, Valid: true}
						// Handle array type: _text -> text[] , _int4 -> integer[]
						// Not support array size limits and array size limits because:
						// https://www.postgresql.org/docs/current/arrays.html#ARRAYS-DECLARATION
						if strings.HasPrefix(mc.DataTypeValue.String, "_") {
							mc.DataTypeValue = sql.NullString{String: dataType, Valid: true}
						}
						break
					}
				}
			}
			dataTypeRows.Close()
		}

		return err
	})
	return
}
