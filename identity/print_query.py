
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
VALUES ('AA4','UB5','AA4','UB5', 1, 1),
('AC2','UB7','AC2','UB7', 1, 1)
"""


prev_table = "id_graph_0"
MAX_VERSION = 2
MAX_ITER = 8

for version in range(MAX_VERSION):

    xyz = input("Press Enter:")
    
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

       FROM   id_graph_0_14
       WHERE  version_user_id = 1)) AS
  TMP_GRAPH_0
  )
  UNION
  (SELECT orig_anon_id,
     orig_user_id,
     curr_anon_id,
     curr_user_id,
     version_anon_id,
     version_user_id
   FROM   id_graph_0_14
   WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                   FROM   id_graph_0_14
                   WHERE  version_anon_id = 1)
          OR orig_user_id IN (SELECT orig_user_id
                       FROM   id_graph_0_14
                       WHERE  version_user_id = 1)));


                  DROP TABLE IF EXISTS id_graph_1_1;
                CREATE TABLE id_graph_1_1 AS
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

               1 AS version_anon_id,
          1 AS  version_user_id

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
                   FROM   id_graph_1_0
                   WHERE  orig_anon_id IN (SELECT orig_anon_id
                           FROM   id_graph_1_0
                           WHERE  version_anon_id = 1)
               OR orig_user_id IN (SELECT orig_user_id
                       FROM   id_graph_1_0
                       WHERE  version_user_id = 1)) AS
          TMP_GRAPH_0
              )
              UNION
              (SELECT orig_anon_id,
               orig_user_id,
               curr_anon_id,
               curr_user_id,
               version_anon_id,
          version_user_id
               FROM   id_graph_1_0
               WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                               FROM   id_graph_1_0
                               WHERE  version_anon_id = 1)
                      OR orig_user_id IN (SELECT orig_user_id
                                   FROM   id_graph_1_0
                                   WHERE  version_user_id = 1)));


                              DROP TABLE IF EXISTS id_graph_1_2;
                            CREATE TABLE id_graph_1_2 AS
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

                           1 AS version_anon_id,
                      1 AS  version_user_id

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
                               FROM   id_graph_1_1
                               WHERE  orig_anon_id IN (SELECT orig_anon_id
                                       FROM   id_graph_1_1
                                       WHERE  version_anon_id = 1)
                           OR orig_user_id IN (SELECT orig_user_id
                                   FROM   id_graph_1_1
                                   WHERE  version_user_id = 1)) AS
                      TMP_GRAPH_0
                          )
                          UNION
                          (SELECT orig_anon_id,
                           orig_user_id,
                           curr_anon_id,
                           curr_user_id,
                           version_anon_id,
                      version_user_id
                           FROM   id_graph_1_1
                           WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                           FROM   id_graph_1_1
                                           WHERE  version_anon_id = 1)
                                  OR orig_user_id IN (SELECT orig_user_id
                                               FROM   id_graph_1_1
                                               WHERE  version_user_id = 1)));


                                          DROP TABLE IF EXISTS id_graph_1_3;
                                        CREATE TABLE id_graph_1_3 AS
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

                                       1 AS version_anon_id,
                                  1 AS  version_user_id

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
                                           FROM   id_graph_1_2
                                           WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                   FROM   id_graph_1_2
                                                   WHERE  version_anon_id = 1)
                                       OR orig_user_id IN (SELECT orig_user_id
                                               FROM   id_graph_1_2
                                               WHERE  version_user_id = 1)) AS
                                  TMP_GRAPH_0
                                      )
                                      UNION
                                      (SELECT orig_anon_id,
                                       orig_user_id,
                                       curr_anon_id,
                                       curr_user_id,
                                       version_anon_id,
                                  version_user_id
                                       FROM   id_graph_1_2
                                       WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                       FROM   id_graph_1_2
                                                       WHERE  version_anon_id = 1)
                                              OR orig_user_id IN (SELECT orig_user_id
                                                           FROM   id_graph_1_2
                                                           WHERE  version_user_id = 1)));


                                                      DROP TABLE IF EXISTS id_graph_1_4;
                                                    CREATE TABLE id_graph_1_4 AS
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

                                                   1 AS version_anon_id,
                                              1 AS  version_user_id

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
                                                       FROM   id_graph_1_3
                                                       WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                               FROM   id_graph_1_3
                                                               WHERE  version_anon_id = 1)
                                                   OR orig_user_id IN (SELECT orig_user_id
                                                           FROM   id_graph_1_3
                                                           WHERE  version_user_id = 1)) AS
                                              TMP_GRAPH_0
                                                  )
                                                  UNION
                                                  (SELECT orig_anon_id,
                                                   orig_user_id,
                                                   curr_anon_id,
                                                   curr_user_id,
                                                   version_anon_id,
                                              version_user_id
                                                   FROM   id_graph_1_3
                                                   WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                   FROM   id_graph_1_3
                                                                   WHERE  version_anon_id = 1)
                                                          OR orig_user_id IN (SELECT orig_user_id
                                                                       FROM   id_graph_1_3
                                                                       WHERE  version_user_id = 1)));


                                                                  DROP TABLE IF EXISTS id_graph_1_5;
                                                                CREATE TABLE id_graph_1_5 AS
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

                                                               1 AS version_anon_id,
                                                          1 AS  version_user_id

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
                                                                   FROM   id_graph_1_4
                                                                   WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                           FROM   id_graph_1_4
                                                                           WHERE  version_anon_id = 1)
                                                               OR orig_user_id IN (SELECT orig_user_id
                                                                       FROM   id_graph_1_4
                                                                       WHERE  version_user_id = 1)) AS
                                                          TMP_GRAPH_0
                                                              )
                                                              UNION
                                                              (SELECT orig_anon_id,
                                                               orig_user_id,
                                                               curr_anon_id,
                                                               curr_user_id,
                                                               version_anon_id,
                                                          version_user_id
                                                               FROM   id_graph_1_4
                                                               WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                               FROM   id_graph_1_4
                                                                               WHERE  version_anon_id = 1)
                                                                      OR orig_user_id IN (SELECT orig_user_id
                                                                                   FROM   id_graph_1_4
                                                                                   WHERE  version_user_id = 1)));


                                                                              DROP TABLE IF EXISTS id_graph_1_6;
                                                                            CREATE TABLE id_graph_1_6 AS
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

                                                                           1 AS version_anon_id,
                                                                      1 AS  version_user_id

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
                                                                               FROM   id_graph_1_5
                                                                               WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                       FROM   id_graph_1_5
                                                                                       WHERE  version_anon_id = 1)
                                                                           OR orig_user_id IN (SELECT orig_user_id
                                                                                   FROM   id_graph_1_5
                                                                                   WHERE  version_user_id = 1)) AS
                                                                      TMP_GRAPH_0
                                                                          )
                                                                          UNION
                                                                          (SELECT orig_anon_id,
                                                                           orig_user_id,
                                                                           curr_anon_id,
                                                                           curr_user_id,
                                                                           version_anon_id,
                                                                      version_user_id
                                                                           FROM   id_graph_1_5
                                                                           WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                           FROM   id_graph_1_5
                                                                                           WHERE  version_anon_id = 1)
                                                                                  OR orig_user_id IN (SELECT orig_user_id
                                                                                               FROM   id_graph_1_5
                                                                                               WHERE  version_user_id = 1)));


                                                                                          DROP TABLE IF EXISTS id_graph_1_7;
                                                                                        CREATE TABLE id_graph_1_7 AS
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

                                                                                       1 AS version_anon_id,
                                                                                  1 AS  version_user_id

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
                                                                                           FROM   id_graph_1_6
                                                                                           WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                   FROM   id_graph_1_6
                                                                                                   WHERE  version_anon_id = 1)
                                                                                       OR orig_user_id IN (SELECT orig_user_id
                                                                                               FROM   id_graph_1_6
                                                                                               WHERE  version_user_id = 1)) AS
                                                                                  TMP_GRAPH_0
                                                                                      )
                                                                                      UNION
                                                                                      (SELECT orig_anon_id,
                                                                                       orig_user_id,
                                                                                       curr_anon_id,
                                                                                       curr_user_id,
                                                                                       version_anon_id,
                                                                                  version_user_id
                                                                                       FROM   id_graph_1_6
                                                                                       WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                       FROM   id_graph_1_6
                                                                                                       WHERE  version_anon_id = 1)
                                                                                              OR orig_user_id IN (SELECT orig_user_id
                                                                                                           FROM   id_graph_1_6
                                                                                                           WHERE  version_user_id = 1)));


                                                                                                      DROP TABLE IF EXISTS id_graph_1_8;
                                                                                                    CREATE TABLE id_graph_1_8 AS
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

                                                                                                   1 AS version_anon_id,
                                                                                              1 AS  version_user_id

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
                                                                                                       FROM   id_graph_1_7
                                                                                                       WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                               FROM   id_graph_1_7
                                                                                                               WHERE  version_anon_id = 1)
                                                                                                   OR orig_user_id IN (SELECT orig_user_id
                                                                                                           FROM   id_graph_1_7
                                                                                                           WHERE  version_user_id = 1)) AS
                                                                                              TMP_GRAPH_0
                                                                                                  )
                                                                                                  UNION
                                                                                                  (SELECT orig_anon_id,
                                                                                                   orig_user_id,
                                                                                                   curr_anon_id,
                                                                                                   curr_user_id,
                                                                                                   version_anon_id,
                                                                                              version_user_id
                                                                                                   FROM   id_graph_1_7
                                                                                                   WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                                   FROM   id_graph_1_7
                                                                                                                   WHERE  version_anon_id = 1)
                                                                                                          OR orig_user_id IN (SELECT orig_user_id
                                                                                                                       FROM   id_graph_1_7
                                                                                                                       WHERE  version_user_id = 1)));


                                                                                                                  DROP TABLE IF EXISTS id_graph_1_9;
                                                                                                                CREATE TABLE id_graph_1_9 AS
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

                                                                                                               1 AS version_anon_id,
                                                                                                          1 AS  version_user_id

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
                                                                                                                   FROM   id_graph_1_8
                                                                                                                   WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                                           FROM   id_graph_1_8
                                                                                                                           WHERE  version_anon_id = 1)
                                                                                                               OR orig_user_id IN (SELECT orig_user_id
                                                                                                                       FROM   id_graph_1_8
                                                                                                                       WHERE  version_user_id = 1)) AS
                                                                                                          TMP_GRAPH_0
                                                                                                              )
                                                                                                              UNION
                                                                                                              (SELECT orig_anon_id,
                                                                                                               orig_user_id,
                                                                                                               curr_anon_id,
                                                                                                               curr_user_id,
                                                                                                               version_anon_id,
                                                                                                          version_user_id
                                                                                                               FROM   id_graph_1_8
                                                                                                               WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                                               FROM   id_graph_1_8
                                                                                                                               WHERE  version_anon_id = 1)
                                                                                                                      OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                   FROM   id_graph_1_8
                                                                                                                                   WHERE  version_user_id = 1)));


                                                                                                                              DROP TABLE IF EXISTS id_graph_1_10;
                                                                                                                            CREATE TABLE id_graph_1_10 AS
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

                                                                                                                           1 AS version_anon_id,
                                                                                                                      1 AS  version_user_id

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
                                                                                                                               FROM   id_graph_1_9
                                                                                                                               WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                       FROM   id_graph_1_9
                                                                                                                                       WHERE  version_anon_id = 1)
                                                                                                                           OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                   FROM   id_graph_1_9
                                                                                                                                   WHERE  version_user_id = 1)) AS
                                                                                                                      TMP_GRAPH_0
                                                                                                                          )
                                                                                                                          UNION
                                                                                                                          (SELECT orig_anon_id,
                                                                                                                           orig_user_id,
                                                                                                                           curr_anon_id,
                                                                                                                           curr_user_id,
                                                                                                                           version_anon_id,
                                                                                                                      version_user_id
                                                                                                                           FROM   id_graph_1_9
                                                                                                                           WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                           FROM   id_graph_1_9
                                                                                                                                           WHERE  version_anon_id = 1)
                                                                                                                                  OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                               FROM   id_graph_1_9
                                                                                                                                               WHERE  version_user_id = 1)));


                                                                                                                                          DROP TABLE IF EXISTS id_graph_1_11;
                                                                                                                                        CREATE TABLE id_graph_1_11 AS
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

                                                                                                                                       1 AS version_anon_id,
                                                                                                                                  1 AS  version_user_id

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
                                                                                                                                           FROM   id_graph_1_10
                                                                                                                                           WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                   FROM   id_graph_1_10
                                                                                                                                                   WHERE  version_anon_id = 1)
                                                                                                                                       OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                               FROM   id_graph_1_10
                                                                                                                                               WHERE  version_user_id = 1)) AS
                                                                                                                                  TMP_GRAPH_0
                                                                                                                                      )
                                                                                                                                      UNION
                                                                                                                                      (SELECT orig_anon_id,
                                                                                                                                       orig_user_id,
                                                                                                                                       curr_anon_id,
                                                                                                                                       curr_user_id,
                                                                                                                                       version_anon_id,
                                                                                                                                  version_user_id
                                                                                                                                       FROM   id_graph_1_10
                                                                                                                                       WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                       FROM   id_graph_1_10
                                                                                                                                                       WHERE  version_anon_id = 1)
                                                                                                                                              OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                                           FROM   id_graph_1_10
                                                                                                                                                           WHERE  version_user_id = 1)));


                                                                                                                                                      DROP TABLE IF EXISTS id_graph_1_12;
                                                                                                                                                    CREATE TABLE id_graph_1_12 AS
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

                                                                                                                                                   1 AS version_anon_id,
                                                                                                                                              1 AS  version_user_id

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
                                                                                                                                                       FROM   id_graph_1_11
                                                                                                                                                       WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                               FROM   id_graph_1_11
                                                                                                                                                               WHERE  version_anon_id = 1)
                                                                                                                                                   OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                                           FROM   id_graph_1_11
                                                                                                                                                           WHERE  version_user_id = 1)) AS
                                                                                                                                              TMP_GRAPH_0
                                                                                                                                                  )
                                                                                                                                                  UNION
                                                                                                                                                  (SELECT orig_anon_id,
                                                                                                                                                   orig_user_id,
                                                                                                                                                   curr_anon_id,
                                                                                                                                                   curr_user_id,
                                                                                                                                                   version_anon_id,
                                                                                                                                              version_user_id
                                                                                                                                                   FROM   id_graph_1_11
                                                                                                                                                   WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                                   FROM   id_graph_1_11
                                                                                                                                                                   WHERE  version_anon_id = 1)
                                                                                                                                                          OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                                                       FROM   id_graph_1_11
                                                                                                                                                                       WHERE  version_user_id = 1)));


                                                                                                                                                                  DROP TABLE IF EXISTS id_graph_1_13;
                                                                                                                                                                CREATE TABLE id_graph_1_13 AS
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

                                                                                                                                                               1 AS version_anon_id,
                                                                                                                                                          1 AS  version_user_id

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
                                                                                                                                                                   FROM   id_graph_1_12
                                                                                                                                                                   WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                                           FROM   id_graph_1_12
                                                                                                                                                                           WHERE  version_anon_id = 1)
                                                                                                                                                               OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                                                       FROM   id_graph_1_12
                                                                                                                                                                       WHERE  version_user_id = 1)) AS
                                                                                                                                                          TMP_GRAPH_0
                                                                                                                                                              )
                                                                                                                                                              UNION
                                                                                                                                                              (SELECT orig_anon_id,
                                                                                                                                                               orig_user_id,
                                                                                                                                                               curr_anon_id,
                                                                                                                                                               curr_user_id,
                                                                                                                                                               version_anon_id,
                                                                                                                                                          version_user_id
                                                                                                                                                               FROM   id_graph_1_12
                                                                                                                                                               WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                                               FROM   id_graph_1_12
                                                                                                                                                                               WHERE  version_anon_id = 1)
                                                                                                                                                                      OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                                                                   FROM   id_graph_1_12
                                                                                                                                                                                   WHERE  version_user_id = 1)));


                                                                                                                                                                              DROP TABLE IF EXISTS id_graph_1_14;
                                                                                                                                                                            CREATE TABLE id_graph_1_14 AS
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

                                                                                                                                                                           1 AS version_anon_id,
                                                                                                                                                                      1 AS  version_user_id

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
                                                                                                                                                                               FROM   id_graph_1_13
                                                                                                                                                                               WHERE  orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                                                       FROM   id_graph_1_13
                                                                                                                                                                                       WHERE  version_anon_id = 1)
                                                                                                                                                                           OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                                                                   FROM   id_graph_1_13
                                                                                                                                                                                   WHERE  version_user_id = 1)) AS
                                                                                                                                                                      TMP_GRAPH_0
                                                                                                                                                                          )
                                                                                                                                                                          UNION
                                                                                                                                                                          (SELECT orig_anon_id,
                                                                                                                                                                           orig_user_id,
                                                                                                                                                                           curr_anon_id,
                                                                                                                                                                           curr_user_id,
                                                                                                                                                                           version_anon_id,
                                                                                                                                                                      version_user_id
                                                                                                                                                                           FROM   id_graph_1_13
                                                                                                                                                                           WHERE  NOT (orig_anon_id IN (SELECT orig_anon_id
                                                                                                                                                                                           FROM   id_graph_1_13
                                                                                                                                                                                           WHERE  version_anon_id = 1)
                                                                                                                                                                                  OR orig_user_id IN (SELECT orig_user_id
                                                                                                                                                                                               FROM   id_graph_1_13
                                                                                                                                                                                               WHERE  version_user_id = 1)));

                                                                                                                                                                          ~print("*********************************\n")
        


