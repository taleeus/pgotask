package pgotask

import "testing"

func TestExtractors(t *testing.T) {
	if tableName := table[Task](); tableName != "task_v1" {
		t.Errorf("wrong table name (%s)", tableName)
	}

	if columns := columns[Task](false, "id", "type", "type_version"); len(columns) != 3 {
		t.Errorf("wrong columns length (%d)", len(columns))
	}

	columnTag := "task_id"
	if column := column[TaskFailure](columnTag); column != columnTag {
		t.Errorf("wrong column tag (%s)", column)
	}
}
