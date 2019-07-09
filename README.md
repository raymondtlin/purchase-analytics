# purchase-analytics

## Methodology

Production-level code generally indicates that resources will be a constraint, particularly memory given scarcity (relative to CPU 
/ Disk IO).  This suggests that libraries such as pandas are to be avoided since: 
1. Data is loaded completely into memory. 
2. Reliance on external libraries reduces portability.
3. External libraries create additional dependencies in an environment.

Natural choice is to use the following standard libraries: csv, itertools, and collections.

Primary approach taken here is to use the Iterator pattern.  By implementing generators, we process rows one at a time, allowing us to manage memory efficiently.  csv.DictReader is abstracted as a Class, which allows us to attach methods to walk through the dataset, row by row.  If I had more time, I would have also considered storing the generated rows as NamedTuples since the schema is largely static.  An additional benefit of using the NamedTuples class would be the reduced memory overhead of implementing `__slots__` as opposed to allocating memory for key:value pairs in a dict.

Having established the 'producers' in our ecosystem using generators, the next step was to implement the consumers.  Itertools and collections were imported for their utility here.  The challenge here is finding a way to aggregate data that streams in row by row (for someone that comes primarily from a SQL background, this was the toughest part to wrap my head around) with minimal writes to memory and none to disk.  For the first-time-orders metric, I considered writing a seperate stream to disk of filtered rows but that solution is not ideal, especially if the disk I'm writing to is the same I'm reading from; as we scale up the amount of reads/writes if the source/destination is the same disk we will likely incur disk thrashing  

### Aggregation Approach

Departments(1) -> Products(Many) -> Orders (Many)

Each product has only one department, while each department holds many products.
Each order has at least one product.  Any product can appear on any order, which suggests (many-to-many relationship).

Since Departments-Products is 1:n, we can create a lookup table between departments and products to order_products, in order to determine the number of orders by department.  Assuming that all products have departments, and the integrity of the foreign key relationship between products and order_products is sound, what we can do to prevent joining on a multi-million row data table is aggregate order_products and group by product_id, then join products for the department_id which in SQL would look like:
```
    with agg as ( SELECT product_id
                      , count(*) as num_orders
                      , sum(case when reordered = '0' then 1 else 0 end) as first_time_orders 
                   FROM order_products
               GROUP BY 1)
          SELECT department_id
                , sum(num_orders) as num_orders
                , sum(first_time_orders) as first_time_orders
                , CAST(sum(first_time_orders)/sum(num_orders) as decimal(5,2)) as percentage
            FROM products
            JOIN agg USING (product_id)
        GROUP BY 1
        ```

This is pretty intuitive in SQL, but not so much in terms of Stream Processing via Python (for me).  Especially since I don't have access to pandas, which means I had to find a way to run multiple aggregations without having to reset the generator (i.e. read from disk again).

I contemplated using tee but was unable to retrieve consistent results.  Given the limitation to one aggregation per processing step.  My approach then became: group orders by product_id, group first_time_orders by product_id, create the map, and then join everything together, which is not ideal.

### Writing
Was not able to complete this in time.

## Source
“The Instacart Online Grocery Shopping Dataset 2017”, Accessed from https://www.instacart.com/datasets/grocery-shopping-2017 on 2019-07-08
nt
