import based/sql
import based/sql/column
import based/sql/condition
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

  let condition =
    column.new("id")
    |> column.eq(val, of: condition.value)

  let expected = "id = :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.int(1)] == condition.to_values(condition, value.text)
}

pub fn greater_than_test() {
  let val = value.int(18)

  let condition =
    column.new("age")
    |> column.gt(val, of: condition.value)

  let expected = "age > :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.int(18)] == condition.to_values(condition, value.text)
}

pub fn less_than_test() {
  let val = value.int(65)

  let condition =
    column.new("age")
    |> column.lt(val, of: condition.value)

  let expected = "age < :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.int(65)] == condition.to_values(condition, value.text)
}

pub fn greater_than_equal_test() {
  let val = value.int(18)

  let condition =
    column.new("age")
    |> column.gt_eq(val, of: condition.value)

  let expected = "age >= :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.int(18)] == condition.to_values(condition, value.text)
}

pub fn less_than_equal_test() {
  let val = value.int(65)

  let condition =
    column.new("age")
    |> column.lt_eq(val, of: condition.value)

  let expected = "age <= :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.int(65)] == condition.to_values(condition, value.text)
}

pub fn not_equal_test() {
  let val = value.text("inactive")

  let condition =
    column.new("status")
    |> column.not_eq(val, of: condition.value)

  let expected = "status <> :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.text("inactive")] == condition.to_values(condition, value.text)
}

pub fn between_test() {
  let start = value.float(10.0)
  let end = value.float(50.0)

  let condition =
    column.new("price")
    |> column.between(start, end, of: condition.value)

  let expected = "price BETWEEN :param AND :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.float(10.0), value.float(50.0)]
    == condition.to_values(condition, value.text)
}

pub fn like_test() {
  let condition =
    column.new("name")
    |> column.like("%John%")

  let expected = "name LIKE :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [value.text("%John%")] == condition.to_values(condition, value.text)
}

// pub fn in_test() {
//   let values = [1, 2, 3]
// 
//   let condition =
//     column.new("id")
//     |> column.in(values, of: sql.list(_, value.int))
// 
//   let expected = "id IN (:param, :param, :param)"
// 
//   assert expected
//     == condition.to_string(condition, fmt.to_identifier(fmt.new(), _))
//   assert [value.int(1), value.int(2), value.int(3)]
//     == condition.to_values(condition, value.text)
// }

pub fn is_test() {
  let condition =
    column.new("active")
    |> column.is(True)

  let expected = "active IS TRUE"

  assert expected == condition.to_string(condition, fmt.new())

  assert [] == condition.to_values(condition, value.text)
}

pub fn not_like_test() {
  let condition =
    column.new("name")
    |> column.not_like("%admin%")

  let expected = "name NOT LIKE :param"

  assert expected == condition.to_string(condition, fmt.new())

  assert [value.text("%admin%")] == condition.to_values(condition, value.text)
}

pub fn avg_test() {
  let repo = value.repo()

  let avg =
    column.avg("number")
    |> column.to_string(repo)

  assert "AVG(number)" == avg
}

pub fn count_test() {
  let repo = value.repo()

  let count =
    column.count("number")
    |> column.to_string(repo)

  assert "COUNT(number)" == count
}

pub fn max_test() {
  let repo = value.repo()

  let max =
    column.max("number")
    |> column.to_string(repo)

  assert "MAX(number)" == max
}

pub fn min_test() {
  let repo = value.repo()

  let min =
    column.min("number")
    |> column.to_string(repo)

  assert "MIN(number)" == min
}

pub fn sum_test() {
  let repo = value.repo()

  let sum =
    column.sum("number")
    |> column.to_string(repo)

  assert "SUM(number)" == sum
}

pub fn avg_alias_test() {
  let repo = value.repo()

  let avg =
    column.avg("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "AVG(number) AS num" == avg
}

pub fn count_alias_test() {
  let repo = value.repo()

  let count =
    column.count("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "COUNT(number) AS num" == count
}

pub fn max_alias_test() {
  let repo = value.repo()

  let max =
    column.max("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "MAX(number) AS num" == max
}

pub fn min_alias_test() {
  let repo = value.repo()

  let min =
    column.min("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "MIN(number) AS num" == min
}

pub fn sum_alias_test() {
  let repo = value.repo()

  let sum =
    column.sum("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "SUM(number) AS num" == sum
}
