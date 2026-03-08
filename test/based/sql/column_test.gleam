import based/db
import based/repo
import based/sql
import based/sql/column
import based/sql/condition
import based/sql/internal/fmt
import gleam/list

pub fn column_test() {
  let col = column.new("id")

  let expected = "id"

  assert expected == column.to_string(col, repo.default())
}

pub fn column_alias_test() {
  let col = column.new("user_id") |> column.alias("id")

  let expected = "user_id AS id"

  assert expected == column.to_string(col, repo.default())
}

pub fn column_with_table_test() {
  let users = sql.table("users")

  let col =
    column.new("id")
    |> column.for(users)

  let expected = "users.id"

  assert expected == column.to_string(col, repo.default())
}

pub fn table_and_alias_test() {
  let users = sql.table("users")

  let col =
    column.new("user_id")
    |> column.alias("id")
    |> column.for(users)

  let expected = "users.user_id AS id"

  assert expected == column.to_string(col, repo.default())
}

fn value_comp() -> condition.Comparable(v, v) {
  condition.comparable(fn(val) { #(condition.value, [val]) })
}

pub fn eq_test() {
  let val = db.int(1)

  let #(condition, values) =
    column.new("id")
    |> column.eq(val, of: value_comp)

  let expected = "id = :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.int(1)] == values
}

pub fn greater_than_test() {
  let val = db.int(18)

  let #(condition, values) =
    column.new("age")
    |> column.gt(val, of: value_comp)

  let expected = "age > :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.int(18)] == values
}

pub fn less_than_test() {
  let val = db.int(65)

  let #(condition, values) =
    column.new("age")
    |> column.lt(val, of: value_comp)

  let expected = "age < :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.int(65)] == values
}

pub fn greater_than_equal_test() {
  let val = db.int(18)

  let #(condition, values) =
    column.new("age")
    |> column.gt_eq(val, of: value_comp)

  let expected = "age >= :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.int(18)] == values
}

pub fn less_than_equal_test() {
  let val = db.int(65)

  let #(condition, values) =
    column.new("age")
    |> column.lt_eq(val, of: value_comp)

  let expected = "age <= :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.int(65)] == values
}

pub fn not_equal_test() {
  let val = db.text("inactive")

  let #(condition, values) =
    column.new("status")
    |> column.not_eq(val, of: value_comp)

  let expected = "status <> :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.text("inactive")] == values
}

pub fn between_test() {
  let start = db.float(10.0)
  let end = db.float(50.0)

  let #(condition, values) =
    column.new("price")
    |> column.between(start, end, of: value_comp)

  let expected = "price BETWEEN :param AND :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.float(10.0), db.float(50.0)] == values
}

pub fn like_test() {
  let #(condition, values) =
    column.new("name")
    |> column.like("%John%")

  let expected = "name LIKE :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [] == values
}

pub fn in_test() {
  let values = [1, 2, 3]

  let list_comp = fn(kind: fn(a) -> v) {
    fn() {
      condition.comparable(fn(vals: List(a)) {
        let node =
          vals
          |> list.length
          |> condition.values

        let vals = list.map(vals, kind)

        #(node, vals)
      })
    }
  }

  let #(condition, values) =
    column.new("id")
    |> column.in(values, of: list_comp(db.int))

  let expected = "id IN (:param, :param, :param)"

  assert expected == condition.to_string(condition, fmt.new())
  assert [db.int(1), db.int(2), db.int(3)] == values
}

pub fn is_test() {
  let #(condition, values) =
    column.new("active")
    |> column.is(True)

  let expected = "active IS TRUE"

  assert expected == condition.to_string(condition, fmt.new())

  assert [] == values
}

pub fn not_like_test() {
  let #(condition, values) =
    column.new("name")
    |> column.not_like("%admin%")

  let expected = "name NOT LIKE :param"

  assert expected == condition.to_string(condition, fmt.new())
  assert [] == values
}

pub fn avg_test() {
  let repo = repo.default()

  let avg =
    column.avg("number")
    |> column.to_string(repo)

  assert "AVG(number)" == avg
}

pub fn count_test() {
  let repo = repo.default()

  let count =
    column.count("number")
    |> column.to_string(repo)

  assert "COUNT(number)" == count
}

pub fn max_test() {
  let repo = repo.default()

  let max =
    column.max("number")
    |> column.to_string(repo)

  assert "MAX(number)" == max
}

pub fn min_test() {
  let repo = repo.default()

  let min =
    column.min("number")
    |> column.to_string(repo)

  assert "MIN(number)" == min
}

pub fn sum_test() {
  let repo = repo.default()

  let sum =
    column.sum("number")
    |> column.to_string(repo)

  assert "SUM(number)" == sum
}

pub fn avg_alias_test() {
  let repo = repo.default()

  let avg =
    column.avg("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "AVG(number) AS num" == avg
}

pub fn count_alias_test() {
  let repo = repo.default()

  let count =
    column.count("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "COUNT(number) AS num" == count
}

pub fn max_alias_test() {
  let repo = repo.default()

  let max =
    column.max("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "MAX(number) AS num" == max
}

pub fn min_alias_test() {
  let repo = repo.default()

  let min =
    column.min("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "MIN(number) AS num" == min
}

pub fn sum_alias_test() {
  let repo = repo.default()

  let sum =
    column.sum("number")
    |> column.alias("num")
    |> column.to_string(repo)

  assert "SUM(number) AS num" == sum
}
