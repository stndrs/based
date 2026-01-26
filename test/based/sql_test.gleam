import based/sql
import based/sql/condition
import based/sql/internal/fmt
import based/value

pub fn or_test() {
  let sql1 =
    sql.column("name")
    |> sql.eq(value.text("John"), of: sql.value)

  let sql2 =
    sql.column("email")
    |> sql.eq(value.text("john@example.com"), of: sql.value)

  let #(condition, values) = sql.or(sql1, sql2)

  let expected = "name = :param OR email = :param"
  assert expected == condition.to_string(condition, fmt.new())

  assert [value.text("John"), value.text("john@example.com")] == values
}

pub fn not_test() {
  let sql1 =
    sql.column("active")
    |> sql.eq(value.true, of: sql.value)

  let #(condition, values) = sql.not(sql1)

  let expected = "NOT active = :param"
  assert expected == condition.to_string(condition, fmt.new())

  assert [value.true] == values
}
