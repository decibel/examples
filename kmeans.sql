/*
 * Copyright 2015, Jim Nasby. See also LICENCE
 *
 * Inspired by https://www.periscopedata.com/blog/multi-dimensional-clustering-using-k-means-postgres.html
 *
 * Also reviewed the C code at
 * https://github.com/umitanuki/kmeans-postgresql/blob/master/kmeans.c to
 * verify my understanding of how kmeans works.
 */
CREATE TABLE purchase(
    user_id                 int     PRIMARY KEY
    , number_of_purchases   int
    , total_spend           numeric(10,2)
);
COPY purchase FROM STDIN WITH CSV;
24120,1,19.99
83917,5,11.93
289092,4,14.6
13568,135,626.64
143134,101,576.99
78381,117,509.83
7484,123,775.75
19514,129,684.71
1761537,2,5.99
\.

/*
 * This view creates 2D points/vectors of number of purchases vs total_spend
 */
CREATE OR REPLACE VIEW purchase__number_of_purchases__vs__total_spend AS
    SELECT user_id, point(number_of_purchases, total_spend) AS purchase_point
        FROM purchase
;

/*
 * This view creates a number of initial clusters from existing data. The ORDER
 * BY makes this expensive on large data sets but makes it impossible to get
 * the same point twice.
 */
CREATE OR REPLACE VIEW _purchase__num_v_spend__sample AS
    SELECT rank() OVER( ORDER BY random() ) AS cluster_number, purchase_point
        FROM purchase__number_of_purchases__vs__total_spend 
        LIMIT 2
;

/*
 * This view turns the sample data into an array. I created it to make it easy
 * to get each cluster point (by doing s[1], s[2], etc), before I realized I
 * could write the calc view to deal with any number of clusters. The view is
 * not used.
 */
CREATE OR REPLACE VIEW _sample_array AS
    SELECT array( (SELECT purchase_point FROM _purchase__num_v_spend__sample ORDER BY cluster_number) ) AS s
;

/*
 * This view is here just to provide an example of a single iteration. I wrote
 * this before tackling the recursive CTE, because it was easier to understand
 * and test what was going on. This view is not used.
 */
CREATE OR REPLACE VIEW one_iter AS
    SELECT DISTINCT ON( user_id )
            *
        FROM ( SELECT cluster_number, user_id
                        , p.purchase_point<->s.average_point AS distance
                        , @@ lseg( p.purchase_point, s.average_point ) AS midpoint
                        , p.purchase_point AS purchase
                        , s.average_point
                    FROM purchase__number_of_purchases__vs__total_spend p
                        CROSS JOIN (
                            SELECT cluster_number, purchase_point AS average_point
                                FROM _purchase__num_v_spend__sample s
                        ) s
                ) d
        ORDER BY user_id, distance
;

/*
 * This view replaces the big recursive CTE in the original post.
 *
 * IF YOU DON'T UNDERSTAND THIS VIEW, study the one_iter view above. All that
 * the recursive CTE does is iterate on the results of the one_iter view!
 */
CREATE OR REPLACE VIEW calc AS
    WITH RECURSIVE kmeans(iter, user_id, cluster_number, average_point) as (
        SELECT 1, NULL::int, *
        FROM _purchase__num_v_spend__sample
        UNION ALL
        SELECT iter + 1, user_id, cluster_number, midpoint
            FROM (

    -- Cut and paste from one_iteration view
        SELECT DISTINCT ON( iter, user_id )
                *
            FROM ( SELECT iter, cluster_number, p.user_id
                            , p.purchase_point<->s.average_point AS distance
                            , @@ lseg( p.purchase_point, s.average_point ) AS midpoint
                            , p.purchase_point AS purchase
                            , s.average_point
                        FROM purchase__number_of_purchases__vs__total_spend p
                        -- !!! EXCEPT !!! Replace subselect with kmeans self-reference
                            CROSS JOIN kmeans s
                    ) d
            ORDER BY iter, user_id, distance
                
            ) r
            WHERE iter < 10
    )
        SELECT * FROM kmeans
;

/*
 * This view just selects final output.
 */
CREATE OR REPLACE VIEW final AS
    SELECT p.*, cluster_number
        FROM calc c
            JOIN purchase p USING( user_id )
        WHERE iter = 10
;
