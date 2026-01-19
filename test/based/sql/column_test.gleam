import based/sql
import based/sql/column
import based/sql/expression
import based/sql/internal/fmt
import based/value

pub fn column_test() {
  let col = column.new("id")

  let expected = "id"

  assert expected == column.to_string(col, value.repo())
}

pub fn column_alias_test() {
  let col = column.new("user_id") |> column.alias("id")

  let expected = "user_id AS id"

  assert expected == column.to_string(col, value.repo())
}

pub fn column_with_table_test() {
  let users = sql.table("users")

  let col =
    column.new("id")
    |> column.for(users)

  let expected = "users.id"

  assert expected == column.to_string(col, value.repo())
}

pub fn table_and_alias_test() {
  let users = sql.table("users")

  let col =
    column.new("user_id")
    |> column.alias("id")
    |> column.for(users)

  let expected = "users.user_id AS id"

  assert expected == column.to_string(col, value.repo())
}

pub fn eq_test() {
  let val = value.int(1)

  let expression =
    column.new("id")
    |> column.eq(val, of: sql.value)

  let expected = "id = :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.int(1)] == expression.to_values(expression, value.text)
}

pub fn greater_than_test() {
  let val = value.int(18)

  let expression =
    column.new("age")
    |> column.gt(val, of: sql.value)

  let expected = "age > :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.int(18)] == expression.to_values(expression, value.text)
}

pub fn less_than_test() {
  let val = value.int(65)

  let expression =
    column.new("age")
    |> column.lt(val, of: sql.value)

  let expected = "age < :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.int(65)] == expression.to_values(expression, value.text)
}

pub fn greater_than_equal_test() {
  let val = value.int(18)

  let expression =
    column.new("age")
    |> column.gt_eq(val, of: sql.value)

  let expected = "age >= :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.int(18)] == expression.to_values(expression, value.text)
}

pub fn less_than_equal_test() {
  let val = value.int(65)

  let expression =
    column.new("age")
    |> column.lt_eq(val, of: sql.value)

  let expected = "age <= :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.int(65)] == expression.to_values(expression, value.text)
}

pub fn not_equal_test() {
  let val = value.text("inactive")

  let expression =
    column.new("status")
    |> column.not_eq(val, of: sql.value)

  let expected = "status <> :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.text("inactive")]
    == expression.to_values(expression, value.text)
}

pub fn between_test() {
  let start = value.float(10.0)
  let end = value.float(50.0)

  let expression =
    column.new("price")
    |> column.between(start, end, of: sql.value)

  let expected = "price BETWEEN :param AND :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.float(10.0), value.float(50.0)]
    == expression.to_values(expression, value.text)
}

pub fn like_test() {
  let expression =
    column.new("name")
    |> column.like("%John%")

  let expected = "name LIKE :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.text("%John%")] == expression.to_values(expression, value.text)
}

pub fn in_test() {
  let values = [1, 2, 3]

  let expression =
    column.new("id")
    |> column.in(values, of: sql.list(_, value.int))

  let expected = "id IN (:param, :param, :param)"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))
  assert [value.int(1), value.int(2), value.int(3)]
    == expression.to_values(expression, value.text)
}

pub fn is_test() {
  let expression =
    column.new("active")
    |> column.is(True)

  let expected = "active IS TRUE"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))

  assert [] == expression.to_values(expression, value.text)
}

pub fn not_like_test() {
  let expression =
    column.new("name")
    |> column.not_like("%admin%")

  let expected = "name NOT LIKE :param"

  assert expected
    == expression.to_string(expression, fmt.to_identifier(fmt.new(), _))

  assert [value.text("%admin%")] == expression.to_values(expression, value.text)
}
