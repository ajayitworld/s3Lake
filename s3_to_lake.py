from configparser import ConfigParser

import cli_base
import csv

# can be improved to create the database based upon the source db as opposed to placing all tables in 1 db
class S3_To_Lake:

    def __init__(self):
        config_object = ConfigParser()
        config_object.read(r'configparam.ini')
        client_info = config_object['client_info']
        self.mapping_file_path = client_info["MAPPING_FILE_PATH"]
        self.upsolver_token = client_info["UPSOLVER_TOKEN"]
        self.glue = client_info["GLUE_CATALOG"]
        self.db_name = client_info["DB_NAME"]
        self.compute_cluster = client_info["COMPUTE_CLUSTER"]
        self.job_prefix = client_info["JOB_PREFIX"]
        self.s3_conn = client_info["S3_CONNECTION"]
        self.bucket_table_mapping = {}

    def read_mapping(self):
        with open(self.mapping_file_path, 'r') as data:
            for line in csv.reader(data):
                self.bucket_table_mapping[line[0].strip(' ')] = line[1].strip(' ')

    def cli_run(self, cmd):
        #print(cmd)
        return cli_base.run(cmd, self.upsolver_token)

    def existsTable(self, table_name):
        cmd = """SELECT count(1) as count FROM {GLUE_CATALOG}.information_schema.tables where table_schema = '{DB}' and table_name = '{TABLE_NAME}'"""\
            .format(GLUE_CATALOG=self.glue, DB=self.db_name, TABLE_NAME=table_name)

        output = self.cli_run(cmd)
        if output[0]:
            return True if int(output[1][0]["count"]) > 0 else False
        else:
            #print(cmd)
            return False

    def dropJob(self, table):
        cmd = """ 
        DROP JOB {JOB_PREFIX}_{TABLE_NAME}_job 
        """.format(TABLE_NAME=table, JOB_PREFIX=self.job_prefix)

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False

    def dropTable(self, table_name):

        cmd = """ 
        DROP TABLE {GLUE_CATALOG}.{DB}.{TABLE}
        DELETE_DATA = true
        COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}" 
        """.format(GLUE_CATALOG=self.glue, DB=self.db_name, TABLE=table_name,
                   COMPUTE_CLUSTER=self.compute_cluster)

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False

    def createCopyJob(self, bucket, table_name):

        cmd = """        
        CREATE SYNC JOB {JOB_PREFIX}_{TABLE_NAME}_job 
            COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}"
            CONTENT_TYPE = AUTO
        AS 
            COPY FROM {S3_CONN}
            LOCATION = '{S3_BUCKET}'
            INTO {GLUE_CATALOG}.{DB}.{TABLE_NAME}
        """.format(GLUE_CATALOG=self.glue, DB=self.db_name, TABLE_NAME=table_name,
                   COMPUTE_CLUSTER=self.compute_cluster, S3_CONN=self.s3_conn,
                   S3_BUCKET = bucket,JOB_PREFIX=self.job_prefix
                   )

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            #print(cmd)
            return False

    def createTable(self, table_name):
        cmd = """ 
        CREATE ICEBERG TABLE 
                {GLUE_CATALOG}.{DB}.{TABLE_NAME}
        COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}" 
        """.format(GLUE_CATALOG=self.glue, DB=self.db_name, TABLE_NAME=table_name,
                   COMPUTE_CLUSTER=self.compute_cluster)

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            return False

    def process(self):
        self.read_mapping()

        if self.bucket_table_mapping:
            for bucket, table in self.bucket_table_mapping.items():
                print(bucket, table)
                if not self.existsTable(table):
                    self.createTable(table)
                    self.createCopyJob(bucket, table)
