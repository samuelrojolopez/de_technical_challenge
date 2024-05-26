# Warehouse Models

At the current state of art of several technologies, lake-houses have become quite relevant and columnar storage
has promoted the de-normalization of analytical environments and consults over the data lakes raw sources, and create
specific meshes for businesses areas. 

The approach I get to this challenge is that, having "frequent question scenarios" allow us to generate data products
that are able to solve general business needs, without the over head of forcing the adoption of a kimball or inmon 
traditional modelling approaches.

Worth to mention, this strategy is heavily caused by time constraints and "limited" tables fields, whereas a SCD
strategy could be helpful in an events data, whereas a business could definitely have a more complex approach where 
a more complex model could bring a lot of benefits doing a relational model and not limiting a single denormalized 
approach where is not required or useful at all.


![alt text](https://github.com/samuelrojolopez/de_technical_challenge/blob/main/src/warehouse/movements_erd.png?raw=true)



## Movements Model

The movements Model is a table that will allow to answer the questions of the deposit and withdrawal movements,
by user, time, categories such as currency, or to validate the status and consult the event_ids for detailed references.

The main drawbacks of this table is that it will grow fast and become quite "tall", so for aggregations could make 
sense to do an adjustment in positive and negative values between withdrawals and deposits and also implement partitions
to enhance performance when filtering, but being quite expensive and slow for users that won't be careful with columns
selection and filtering their queries.

This partitioning could also be heavily useful if the information is append only, allowing the historical data to be
stored and later on moved into cold-storage strategies in _historical versions of the tables.

## Login Events

The login events has in mind that even when the events could be a tall useful table as is, common questions being
repeated several times a day (or even by hour) could have a downstream table that could allow other teams to 
instantly consult an specific user last event for their specific fields.

Why is this beneficial?, because the table will be as big as our users existence and allow aggregations on last events
in a simple way, and also having quick look ups into their last events, such as logins, that could be immediately useful
for many teams, rather than making complex queries for consulting only the latest information of specific events.

This should not only improve the query costs vs consulting the raw events table, but also simplify the user last login 
events retrieve by user or datetime into simple queries.





