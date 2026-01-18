import based/sql
import based/sql/column
import based/sql/internal/expr
import based/sql/internal/fmt
import based/sql/internal/node
import based/value

pub fn and_test() {
  let sql1 =
    sql.column("active")
    |> column.eq(value.true, of: sql.value)

  let sql2 =
    sql.column("age")
    |> column.gt(value.int(18), of: sql.value)

  let and_sql = sql.and(sql1, sql2)

  let expected = "active = :param AND age > :param"
  assert expected == expr.to_string(and_sql, fmt.to_identifier(fmt.new(), _))

  assert [value.true, value.int(18)] == expr.to_values(and_sql, value.text)
}

pub fn or_test() {
  let sql1 =
    sql.column("name")
    |> column.eq(value.text("John"), of: sql.value)

  let sql2 =
    sql.column("email")
    |> column.eq(value.text("john@example.com"), of: sql.value)

  let or_sql = sql.or(sql1, sql2)

  let expected = "name = :param OR email = :param"
  assert expected == expr.to_string(or_sql, fmt.to_identifier(fmt.new(), _))

  assert [value.text("John"), value.text("john@example.com")]
    == expr.to_values(or_sql, value.text)
}

pub fn not_test() {
  let sql1 =
    sql.column("active")
    |> column.eq(value.true, of: sql.value)

  let not_sql = sql.not(sql1)

  let expected = "NOT active = :param"
  assert expected == expr.to_string(not_sql, fmt.to_identifier(fmt.new(), _))

  assert [value.true] == expr.to_values(not_sql, value.text)
}

pub fn complex_sqlession_test() {
  let sql1 =
    sql.column("active")
    |> column.eq(value.true, of: sql.value)

  let sql2 =
    sql.column("age")
    |> column.gt(value.int(18), of: sql.value)

  let sql3 =
    sql.column("role")
    |> column.eq(value.text("admin"), of: sql.value)

  let and_sql = sql.and(sql1, sql2)
  let or_sql = sql.or(and_sql, sql3)

  let expected = "active = :param AND age > :param OR role = :param"
  assert expected == expr.to_string(or_sql, fmt.to_identifier(fmt.new(), _))

  assert [value.true, value.int(18), value.text("admin")]
    == expr.to_values(or_sql, value.text)
}

pub fn value_test() {
  let val = sql.value(value.int(42))

  assert ":param" == node.to_string(val, fmt.to_identifier(fmt.new(), _))

  assert [value.int(42)] == node.unwrap(val, value.text)
}

// pub fn values_test() {
//   let vals = sql.values([value.int(1), value.int(2), value.int(3)])
// 
//   let expected = "(:param, :param, :param)"
//   sql.node_to_string(vals, value.format())
//   |> should.equal(expected)
// 
//   sql.unwrap(vals)
//   |> should.equal([value.int(1), value.int(2), value.int(3)])
// }
// 
// pub fn tuples_test() {
//   let tuples =
//     sql.tuples([
//       [sql.value(value.int(1)), sql.value(value.text("John"))],
//       [sql.value(value.int(2)), sql.value(value.text("Jane"))],
//     ])
// 
//   let expected = "((:param, :param), (:param, :param))"
//   sql.node_to_string(tuples, value.format())
//   |> should.equal(expected)
// 
//   sql.unwrap(tuples)
//   |> should.equal([
//     value.int(1),
//     value.text("John"),
//     value.int(2),
//     value.text("Jane"),
//   ])
// }
// 
pub fn special_values_test() {
  let true_node = sql.value(value.true)
  let false_node = sql.value(value.false)
  let null_node = sql.value(value.null)

  assert ":param" == node.to_string(true_node, fmt.to_identifier(fmt.new(), _))
  assert ":param" == node.to_string(false_node, fmt.to_identifier(fmt.new(), _))
  assert ":param" == node.to_string(null_node, fmt.to_identifier(fmt.new(), _))

  assert [value.true] == node.unwrap(true_node, value.text)
  assert [value.false] == node.unwrap(false_node, value.text)
  assert [value.null] == node.unwrap(null_node, value.text)
}
