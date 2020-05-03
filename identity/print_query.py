
"""
DROP TABLE IF EXISTS ID_GRAPH_0;
CREATE TABLE ID_GRAPH_0 (
  orig_anon_id varchar(32), 
  orig_user_id varchar(32), 
  curr_anon_id varchar(32), 
  curr_user_id varchar(32),
  version_anon_id int,
  version_user_id int
); 

#3 connected graphs

INSERT INTO ID_GRAPH_0
 VALUES 
 ('AA2', 'UA1', 'AA2', 'UA1', 0, 0),
 ('AA2', 'UA3', 'AA2', 'UA3', 0, 0),
 ('AA4', 'UA3', 'AA4', 'UA3', 0, 0),
 ('AA4', 'UA5', 'AA4', 'UA5', 0, 0),
 ('AA6', 'UA5', 'AA6', 'UA5', 0, 0),
 ('AA6', 'UA7', 'AA6', 'UA7', 0, 0),
 ('AA8', 'UA7', 'AA8', 'UA7', 0, 0),


 ('AB2', 'UB1', 'AB2', 'UB1', 0, 0),
 ('AB2', 'UB3', 'AB2', 'UB3', 0, 0),
 ('AB4', 'UB3', 'AB4', 'UB3', 0, 0),
 ('AB4', 'UB5', 'AB4', 'UB5', 0, 0),
 ('AB6', 'UB5', 'AB6', 'UB5', 0, 0),
 ('AB6', 'UB7', 'AB6', 'UB7', 0, 0),
 ('AB8', 'UB7', 'AB8', 'UB7', 0, 0),


 ('AC2', 'UC1', 'AC2', 'UC1', 0, 0),
 ('AC2', 'UC3', 'AC2', 'UC3', 0, 0),
 ('AC4', 'UC3', 'AC4', 'UC3', 0, 0),
 ('AC4', 'UC5', 'AC4', 'UC5', 0, 0),
 ('AC6', 'UC5', 'AC6', 'UC5', 0, 0),
 ('AC6', 'UC7', 'AC6', 'UC7', 0, 0),
 ('AC8', 'UC7', 'AC8', 'UC7', 0, 0);


==========
#Connect them in version 1

INSERT INTO ID_GRAPH_0_7
VALUES ('AA4','UB5','AA4','UB5', 1, 1);

#Connect them in version 2
INSERT INTO ID_GRAPH_1_7
VALUES ('AC2','UB7','AC2','UB7', 2, 2);
"""


prev_table = "id_graph_0"
base_table = prev_table
MAX_VERSION = 3
MAX_ITER = 8
NO_DELTA = True
DELTA_FOR_EVER = True

for version in range(MAX_VERSION):

    xyz = input("Press Enter:")
    
    for cnt in range(MAX_ITER):
        
        next_table = "id_graph_%d_%d" % (version, cnt)
        format_str = {"curr_version":str(version), "next_table": next_table, "prev_table": prev_table, "base_table":base_table}

        if NO_DELTA:
          query = """
                DROP TABLE IF EXISTS {next_table};
		CREATE TABLE {next_table} AS
		(
		 SELECT 
		orig_anon_id, 
		orig_user_id, 
			CASE 
			WHEN curr_anon_id is NULL THEN NULL
			WHEN curr_anon_id > tmp_anon_id THEN tmp_anon_id
			ELSE curr_anon_id
		     END AS curr_anon_id,

		     CASE 
			WHEN curr_user_id is NULL THEN NULL
			WHEN curr_user_id > tmp_user_id THEN tmp_user_id
			ELSE curr_user_id
		     END as curr_user_id,
                     0 as version_anon_id,
                     0 as version_user_id
		FROM
		 (
		  SELECT 
			    orig_anon_id,
			    orig_user_id, 
			    curr_anon_id,
			  curr_user_id,
		    MIN(curr_user_id) 
		  OVER(PARTITION BY orig_anon_id)
		    as tmp_anon_id,

		    MIN(curr_anon_id) 
			      OVER(PARTITION BY orig_user_id)
			 as tmp_user_id
		   FROM
		     {prev_table}
		   ) AS TMP_GRAPH_0
		 );
	    """.format(**format_str)
        elif DELTA_FOR_EVER:
            query = """
                DROP TABLE IF EXISTS {next_table};
		CREATE TABLE {next_table} AS
		  (SELECT DISTINCT
			  orig_anon_id,

			  orig_user_id,

			  CASE
			    WHEN curr_anon_id IS NULL THEN NULL
			    WHEN tmp_anon_id < curr_anon_id THEN tmp_anon_id
			    ELSE curr_anon_id
			  END AS curr_anon_id,

			  CASE
			    WHEN curr_user_id IS NULL THEN NULL
			    WHEN tmp_user_id < curr_user_id THEN tmp_user_id
			    ELSE curr_user_id
			  END AS curr_user_id,

			  {curr_version} AS version_anon_id,
			  {curr_version} AS  version_user_id

		   FROM   (SELECT orig_anon_id,
				  orig_user_id,
				  curr_anon_id,
				  curr_user_id,
				  version_anon_id,
				  version_user_id,
				  Min(curr_user_id)
				    over(
				      PARTITION BY orig_anon_id) AS tmp_anon_id,
				  Min(curr_anon_id)
				    over(
				      PARTITION BY orig_user_id) AS tmp_user_id
			   FROM   
                             (SELECT * FROM {prev_table} UNION SELECT * FROM {base_table}) AS TMP_GRAPH_IN
			   WHERE   orig_anon_id IN (SELECT orig_anon_id
						   FROM   {prev_table}
						   WHERE  version_anon_id = {curr_version})
				   OR orig_user_id IN (SELECT orig_user_id
						       FROM   {prev_table}
						       WHERE  version_user_id = {curr_version})) AS
			  TMP_GRAPH_OUTER
		  );
	         """.format(**format_str)
        else:
            query = """
                DROP TABLE IF EXISTS {next_table};
		CREATE TABLE {next_table} AS
		  (SELECT 
			  orig_anon_id,

			  orig_user_id,

			  CASE
			    WHEN curr_anon_id IS NULL THEN NULL
			    WHEN tmp_anon_id < curr_anon_id THEN tmp_anon_id
			    ELSE curr_anon_id
			  END AS curr_anon_id,

			  CASE
			    WHEN curr_user_id IS NULL THEN NULL
			    WHEN tmp_user_id < curr_user_id THEN tmp_user_id
			    ELSE curr_user_id
			  END AS curr_user_id,

			  {curr_version} AS version_anon_id,
			  {curr_version} AS  version_user_id

		   FROM   (SELECT orig_anon_id,
				  orig_user_id,
				  curr_anon_id,
				  curr_user_id,
				  version_anon_id,
				  version_user_id,
				  Min(curr_user_id)
				    over(
				      PARTITION BY orig_anon_id) AS tmp_anon_id,
				  Min(curr_anon_id)
				    over(
				      PARTITION BY orig_user_id) AS tmp_user_id
			   FROM   {prev_table}
			   WHERE  orig_anon_id IN (SELECT orig_anon_id
						   FROM   {prev_table}
						   WHERE  version_anon_id = {curr_version})
				   OR orig_user_id IN (SELECT orig_user_id
						       FROM   {prev_table}
						       WHERE  version_user_id = {curr_version})) AS
			  TMP_GRAPH_0
		  )
		  UNION
		  (SELECT orig_anon_id,
			  orig_user_id,
			  curr_anon_id,
			  curr_user_id,
			  version_anon_id,
			  version_user_id
		   FROM   {prev_table}
		   WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
					   FROM   {prev_table}
					   WHERE  version_anon_id = {curr_version})
			  OR orig_user_id IN (SELECT orig_user_id
					       FROM   {prev_table}
					       WHERE  version_user_id = {curr_version})));
	         """.format(**format_str)
        print(query)
        prev_table = next_table

    #End of version, update TABLE
    if not NO_DELTA and DELTA_FOR_EVER:
        query = """INSERT INTO {next_table} 
                   SELECT * from {base_table} 
                   WHERE  orig_anon_id NOT IN 
                             (SELECT orig_anon_id                                                                                     
                               FROM    {next_table})
                          AND
                         orig_user_id NOT IN 
                             (SELECT orig_user_id                                              
                                FROM {next_table});
                 """.format(**format_str)
        print(query)
        base_table = next_table
