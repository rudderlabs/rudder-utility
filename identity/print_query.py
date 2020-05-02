
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

INSERT INTO ID_GRAPH_0
 VALUES 
 ('A2', 'U1', 'A2', 'U1', 0, 0),
 ('A2', 'U3', 'A2', 'U3', 0, 0),
 ('A4', 'U3', 'A4', 'U3', 0, 0),
 #('A4', 'U5', 'A4', 'U5', 0, 0),
 ('A6', 'U5', 'A6', 'U5', 0, 0),
 ('A6', 'U7', 'A6', 'U7', 0, 0),
 ('A8', 'U7', 'A8', 'U7', 0, 0);

==========
INSERT INTO ID_GRAPH_0_6
VALUES ('A1','U2','A1','U2', 1, 1)
VALUES ('A2','U2','A2','U2', 1, 1)
"""


#version = 0
#prev_table = "id_graph_0"
version = 1
prev_table = "id_graph_0_7"
MAX_ITER = 8

for cnt in range(MAX_ITER):

    next_table = "id_graph_%d_%d" % (version, cnt)
    format_str = {"curr_version":str(version), "next_table": next_table, "prev_table": prev_table}
    
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


