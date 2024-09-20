package pgotask

import (
	"fmt"
	"reflect"
	"strings"
)

type model interface {
	TableName() string
}

func table[M model]() string {
	var model M
	return model.TableName()
}

func column[M model](column string) string {
	var model M
	modelTyp := reflect.TypeOf(model)
	numField := modelTyp.NumField()

	for i := range numField {
		tag, ok := modelTyp.Field(i).Tag.Lookup("db")
		if !ok {
			continue
		}

		if tag != column {
			continue
		}

		return tag
	}

	panic(fmt.Errorf("column %s not present in Model %T", column, model))
}

func prefix(prefix, column string) string {
	return prefix + "." + column
}

func join(columns []string) string {
	return strings.Join(columns, ",\n\t")
}

func columns[M model](tablePrefix bool, columns ...string) []string {
	if len(columns) > 0 {
		columnsOut := make([]string, 0, len(columns))
		for _, c := range columns {
			column := column[M](c)
			if tablePrefix {
				column = prefix(table[M](), column)
			}

			columnsOut = append(columnsOut, column)
		}

		return columnsOut
	}

	var model M
	modelTyp := reflect.TypeOf(model)
	numField := modelTyp.NumField()

	columnTags := make([]string, 0, numField)
	for i := range numField {
		tag, ok := modelTyp.Field(i).Tag.Lookup("db")
		if !ok {
			continue
		}

		if tablePrefix {
			tag = prefix(table[M](), tag)
		}

		columnTags = append(columnTags, tag)
	}

	return columnTags
}
