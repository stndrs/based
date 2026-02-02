import based/db
import based/sql
import based/sql/condition
import based/sql/internal/fmt

pub fn or_test() {
  let sql1 =
    sql.column("name")
    |> sql.eq(db.text("John"), of: sql.val)

  let sql2 =
    sql.column("email")
    |> sql.eq(db.text("john@example.com"), of: sql.val)

  let #(condition, values) = sql.or(sql1, sql2)

  let expected = "name = :param OR email = :param"
  assert expected == condition.to_string(condition, fmt.new())

  assert [db.text("John"), db.text("john@example.com")] == values
}

pub fn not_test() {
  let sql1 =
    sql.column("active")
    |> sql.eq(db.true, of: sql.val)

  let #(condition, values) = sql.not(sql1)

  let expected = "NOT active = :param"
  assert expected == condition.to_string(condition, fmt.new())

  assert [db.true] == values
}
