import based/sql
import based/sql/column
import based/sql/expr
import based/sql/node
import based/value
import gleam/string_tree
import gleeunit/should

pub fn eq_test() {
  let col = column.new("id")
  let lit = node.literal(value.int(1))

  let expr = expr.eq(node.column(col), lit)

  let expected = "id = :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.int(1)])
}

pub fn greater_than_test() {
  let col = column.new("age")
  let lit = node.literal(value.int(18))

  let expr = expr.gt(node.column(col), lit)

  let expected = "age > :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.int(18)])
}

pub fn less_than_test() {
  let col = column.new("age")
  let lit = node.literal(value.int(65))

  let expr = expr.lt(node.column(col), lit)

  let expected = "age < :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.int(65)])
}

pub fn greater_than_equal_test() {
  let col = column.new("age")
  let lit = node.literal(value.int(18))

  let expr = expr.gt_eq(node.column(col), lit)

  let expected = "age >= :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.int(18)])
}

pub fn less_than_equal_test() {
  let col = column.new("age")
  let lit = node.literal(value.int(65))

  let expr = expr.lt_eq(node.column(col), lit)

  let expected = "age <= :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.int(65)])
}

pub fn not_equal_test() {
  let col = column.new("status")
  let lit = node.literal(value.text("inactive"))

  let expr = expr.not_eq(node.column(col), lit)

  let expected = "status <> :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.text("inactive")])
}

pub fn between_test() {
  let col = column.new("price")
  let start = node.literal(value.float(10.0))
  let end = node.literal(value.float(50.0))

  let expr = expr.between(node.column(col), start, end)

  let expected = "price BETWEEN :param AND :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr)
  |> should.equal([value.float(10.0), value.float(50.0)])
}

pub fn like_test() {
  let col = column.new("name")
  let lit = node.literal(value.text("%John%"))

  let expr = expr.like(node.column(col), lit)

  let expected = "name LIKE :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.text("%John%")])
}

pub fn in_test() {
  let col = column.new("id")
  let values = node.literals([value.int(1), value.int(2), value.int(3)])

  let expr = expr.in(node.column(col), values)

  let expected = "id IN (:param, :param, :param)"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr)
  |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn is_test() {
  let col = column.new("active")

  let expr = expr.is(node.column(col), sql.true)

  let expected = "active IS :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.true])
}

pub fn not_like_test() {
  let col = column.new("name")
  let lit = node.literal(value.text("%admin%"))

  let expr = expr.not_like(node.column(col), lit)

  let expected = "name NOT LIKE :param"
  expr.to_string_tree(expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(expr) |> should.equal([value.text("%admin%")])
}

pub fn and_test() {
  let expr1 = expr.eq(node.column(column.new("active")), sql.true)
  let expr2 =
    expr.gt(node.column(column.new("age")), node.literal(value.int(18)))

  let and_expr = expr.and(expr1, expr2)

  let expected = "active = :param AND age > :param"
  expr.to_string_tree(and_expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(and_expr) |> should.equal([value.true, value.int(18)])
}

pub fn or_test() {
  let expr1 =
    expr.eq(node.column(column.new("name")), node.literal(value.text("John")))
  let expr2 =
    expr.eq(
      node.column(column.new("email")),
      node.literal(value.text("john@example.com")),
    )

  let or_expr = expr.or(expr1, expr2)

  let expected = "name = :param OR email = :param"
  expr.to_string_tree(or_expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(or_expr)
  |> should.equal([value.text("John"), value.text("john@example.com")])
}

pub fn not_test() {
  let expr1 = expr.eq(node.column(column.new("active")), sql.true)

  let not_expr = expr.not(expr1)

  let expected = "NOT active = :param"
  expr.to_string_tree(not_expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(not_expr) |> should.equal([value.true])
}

pub fn complex_expression_test() {
  let col1 = column.new("active")
  let col2 = column.new("age")
  let col3 = column.new("role")

  let expr1 = expr.eq(node.column(col1), sql.true)
  let expr2 = expr.gt(node.column(col2), node.literal(value.int(18)))
  let expr3 = expr.eq(node.column(col3), node.literal(value.text("admin")))

  let and_expr = expr.and(expr1, expr2)
  let or_expr = expr.or(and_expr, expr3)

  let expected = "active = :param AND age > :param OR role = :param"
  expr.to_string_tree(or_expr, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  expr.to_values(or_expr)
  |> should.equal([value.true, value.int(18), value.text("admin")])
}
