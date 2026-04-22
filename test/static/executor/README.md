# Test script

``` sql
CREATE TABLE departments (id INT);
CREATE TABLE employees (id INT, department_id INT);

INSERT INTO departments (id) VALUES
(1),
(2),
(3),
(4),
(5);

INSERT INTO employees (id, department_id) VALUES
(1,3),
(2,6),
(3,12),
(4,11),
(5,1),
(6,5),
(33,31),
(66,32),
(67,33),
(68,34),
(69,35);

SELECT departments.id*2, employees.id+1 FROM employees RIGHT JOIN departments ON employees.department_id = departments.id AND departments.id > 3 AND departments.id*2*2/2 < 30;
```

