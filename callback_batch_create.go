package gorm

import (
	"fmt"
	"reflect"

	"strings"
)

// Define callbacks for batch creating
func init() {
	DefaultCallback.BatchCreate().Register("gorm:begin_transaction", beginTransactionCallback)
	DefaultCallback.BatchCreate().Register("gorm:before_batch_create", beforeBatchCreateCallback)
	DefaultCallback.BatchCreate().Register("gorm:update_time_stamp", updateTimeStampForBatchCreateCallback)
	DefaultCallback.BatchCreate().Register("gorm:batch_create", batchCreateCallback)
	DefaultCallback.BatchCreate().Register("gorm:commit_or_rollback_transaction", commitOrRollbackTransactionCallback)
}

func beforeBatchCreateCallback(scope *Scope) {
	if !scope.HasError() {
		indirectScopeValue := scope.IndirectValue()

		if indirectScopeValue.Kind() != reflect.Slice {
			scope.Err(fmt.Errorf("beforeBatchCreateCallback cannot be called for non-slice value, %+v given", indirectScopeValue.Interface()))
			return
		}

		// 只调用第一个对象的方法即可，但要注意BeforeBatchCreate方法内部要自己从scope里取出每个元素去做处理
		// 而不是只处理当前元素，只是借用下那处的代码罢了
		if indirectScopeValue.Len() > 0 {
			scope.callMethod("BeforeBatchCreate", indirectScopeValue.Index(0))
		}
	}
}

// updateTimeStampForBatchCreateCallback will set `CreatedAt`, `UpdatedAt` when creating
func updateTimeStampForBatchCreateCallback(scope *Scope) {
	if !scope.HasError() {
		now := NowFunc()

		indirectScopeValue := scope.IndirectValue()

		if indirectScopeValue.Kind() != reflect.Slice {
			scope.Err(fmt.Errorf("updateTimeStampForBatchCreateCallback cannot be called for non-slice value, %+v given", indirectScopeValue.Interface()))
			return
		}

		// 挨个元素去检查，为空则给予值
		for elementIndex := 0; elementIndex < indirectScopeValue.Len(); elementIndex++ {
			fields := FiledsWithIndexForBatch(scope, elementIndex)
			for _, field := range fields {
				if !field.IsBlank {
					continue
				}

				if field.Name == "CreatedAt" ||
					field.Name == "UpdatedAt" {
					field.Set(now)
				}
			}
		}
	}
}

// batchCreateCallback the callback used to insert data into database
func batchCreateCallback(scope *Scope) {
	if !scope.HasError() {
		defer scope.trace(NowFunc())

		indirectScopeValue := scope.IndirectValue()
		if indirectScopeValue.Kind() != reflect.Slice {
			scope.Err(fmt.Errorf("batchCreateCallback cannot be called for non-slice value, %+v given", indirectScopeValue.Interface()))
			return
		}

		if indirectScopeValue.Len() <= 0 {
			scope.Err(fmt.Errorf("batchCreateCallback cannot be called for empty slice, %+v given", indirectScopeValue.Interface()))
			return
		}

		var (
			columns      []string
			placeholders = make([][]string, indirectScopeValue.Len())
		)

		// 列名获取
		fields := FiledsWithIndexForBatch(scope, 0)
		existColumnNames := map[string]bool{}
		for _, field := range fields {
			if !field.IsNormal || field.IsIgnored {
				continue
			}

			// 因为是批量，要支持各种情况，所以这里就简单的有效列名全都给予
			columns = append(columns, scope.Quote(field.DBName))
			existColumnNames[field.Name] = true
		}

		if len(columns) <= 0 {
			scope.Err(fmt.Errorf("batchCreateCallback cannot be called for empty columns, %+v given", indirectScopeValue.Interface()))
			return
		}

		// 塞入内容，因为是数组，所以需要挨个去塞
		for elementIndex := 0; elementIndex < indirectScopeValue.Len(); elementIndex++ {
			valuePlaceholders := []string{}

			fields := FiledsWithIndexForBatch(scope, elementIndex)
			for _, field := range fields {
				if existColumnNames[field.Name] {
					var v interface{}
					if !field.IsBlank {
						v = field.Field.Interface()
					} else {
						// 如果不是主键
						if !field.IsPrimaryKey {
							// 若有默认值，就直接塞入默认值即可
							if field.HasDefaultValue {
								v = field.TagSettings["DEFAULT"]
								field.Set(v) // 回写原对象
							} else {
								// 没默认值的话，就用原对象值，0啊空字符串什么的
								v = field.Field.Interface()
							}
						}
						// 否则的话v就是nil嘛，然后最终会体现成NULL，能自动支持主键的自增行为
					}

					valuePlaceholders = append(valuePlaceholders, scope.AddToVars(v))
				}
			}

			placeholders[elementIndex] = valuePlaceholders
		}

		// 构造Values语句
		valuePlaceholders := []string{}
		for _, placeholder := range placeholders {
			valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("(%s)", strings.Join(placeholder, ",")))
		}

		// 额外Option
		var extraOption string
		if str, ok := scope.Get("gorm:insert_option"); ok {
			extraOption = fmt.Sprint(str)
		}

		// 构造prepare语句
		scope.Raw(fmt.Sprintf(
			"INSERT INTO %v (%v) VALUES %v%v",
			scope.QuotedTableName(),
			strings.Join(columns, ","),
			strings.Join(valuePlaceholders, ","),
			addExtraSpaceIfExist(extraOption),
		))

		// 执行语句
		if result, err := scope.SQLDB().Exec(scope.SQL, scope.SQLVars...); scope.Err(err) == nil {
			scope.db.RowsAffected, _ = result.RowsAffected()

			// TODO: 因为mysql底层driver不支持批量插入拿取最终insert id 所以这里也就没办法回写了，暂时没招
		}
	}
}

func FiledsWithIndexForBatch(scope *Scope, index int) []*Field {
	indirectScopeValue := scope.IndirectValue()
	if indirectScopeValue.Kind() != reflect.Slice { // 非数组不考虑
		return nil
	}
	if index >= indirectScopeValue.Len() { // 不能越界
		return nil
	}
	indirectScopeValue = reflect.Indirect(indirectScopeValue.Index(index))

	isStruct := indirectScopeValue.Kind() == reflect.Struct
	fields := []*Field{}
	for _, structField := range scope.GetModelStruct().StructFields {
		if isStruct {
			fieldValue := indirectScopeValue
			for _, name := range structField.Names {
				if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
					fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
				}
				fieldValue = reflect.Indirect(fieldValue).FieldByName(name)
			}
			fields = append(fields, &Field{StructField: structField, Field: fieldValue, IsBlank: isBlank(fieldValue)})
		} else {
			fields = append(fields, &Field{StructField: structField, IsBlank: true})
		}
	}
	return fields
}
