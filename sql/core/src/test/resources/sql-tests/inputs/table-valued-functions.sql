-- unresolved function
select * from dummy(3);

-- range call with end
select * from range(6 + cos(3));

-- range call with start and end
select * from range(5, 10);

-- range call with step
select * from range(0, 10, 2);

-- range call with numPartitions
select * from range(0, 10, 1, 200);

-- range call with invalid number of arguments
select * from range(1, 1, 1, 1, 1);

-- range call with null
select * from range(1, null);

-- range call with incompatible type
select * from range(array(1, 2, 3));

-- range call with illegal step
select * from range(0, 5, 0);

-- range call with a mixed-case function name
select * from RaNgE(2);
