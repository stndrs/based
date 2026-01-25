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

  let or_sql = sql.or(sql1, sql2)

  let expected = "name = :param OR email = :param"
  assert expected == condition.to_string(or_sql, fmt.new())

  assert [value.text("John"), value.text("john@example.com")]
    == condition.to_values(or_sql, value.text)
}

pub fn not_test() {
  let sql1 =
    sql.column("active")
    |> sql.eq(value.true, of: sql.value)

  let not_sql = sql.not(sql1)

  let expected = "NOT active = :param"
  assert expected == condition.to_string(not_sql, fmt.new())

  assert [value.true] == condition.to_values(not_sql, value.text)
}

pub fn value_test() {
  let val = sql.value.to_node(value.int(42))

  assert ":param" == condition.node_to_string(val, fmt.new())

  assert [value.int(42)] == condition.node_to_values(val, value.text)
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
  let true_node = sql.value.to_node(value.true)
  let false_node = sql.value.to_node(value.false)
  let null_node = sql.value.to_node(value.null)

  assert ":param" == condition.node_to_string(true_node, fmt.new())
  assert ":param" == condition.node_to_string(false_node, fmt.new())
  assert ":param" == condition.node_to_string(null_node, fmt.new())

  assert [value.true] == condition.node_to_values(true_node, value.text)
  assert [value.false] == condition.node_to_values(false_node, value.text)
  assert [value.null] == condition.node_to_values(null_node, value.text)
}
