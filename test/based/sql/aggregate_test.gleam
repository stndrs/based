import based/sql
import based/sql/aggregate
import based/value

pub fn avg_test() {
  let repo = value.repo()

  let avg =
    sql.column("number")
    |> aggregate.avg
    |> aggregate.to_string(repo)

  assert "AVG(number)" == avg
}

pub fn count_test() {
  let repo = value.repo()

  let count =
    sql.column("number")
    |> aggregate.count
    |> aggregate.to_string(repo)

  assert "COUNT(number)" == count
}

pub fn max_test() {
  let repo = value.repo()

  let max =
    sql.column("number")
    |> aggregate.max
    |> aggregate.to_string(repo)

  assert "MAX(number)" == max
}

pub fn min_test() {
  let repo = value.repo()

  let min =
    sql.column("number")
    |> aggregate.min
    |> aggregate.to_string(repo)

  assert "MIN(number)" == min
}

pub fn sum_test() {
  let repo = value.repo()

  let sum =
    sql.column("number")
    |> aggregate.sum
    |> aggregate.to_string(repo)

  assert "SUM(number)" == sum
}
