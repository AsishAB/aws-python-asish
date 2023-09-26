import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

source_glue_db = "source-db-s3"
source_glue_table = "goals_excelsheet__small_1_csv"
target_s3store = "aws-glue-2222"
redshiftTmpDir_s3="aws-glue-assets-201149591384-ap-south-1"
redshift_connection_name = "db-redshift-connection"
targer_redshift_dbtable = "tbl_goals_redshift.table_goals_redshift"
target_iam_role_redshift = "etl-redshift-asish"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


source_DataCatalog = glueContext.create_dynamic_frame.from_catalog(
    database=source_glue_db,
    table_name=source_glue_table,
    #transformation_ctx="source_Target",
)


apply_Transformation = ApplyMapping.apply(
    frame=source_DataCatalog,
    mappings=[
        ("game_id", "long", "game_id", "bigint"),
        ("minute", "string", "no_of_minute", "int"),
        ("player", "string", "player_name", "string"),
        ("team1score", "long", "team_1_score", "int"),
        ("team2score", "long", "team_2_score", "int"),
    ],
    #transformation_ctx="apply_Transformation",
)

target_s3 = glueContext.write_dynamic_frame.from_options(
    frame=apply_Transformation,
    connection_type="s3",
    format="csv",
    connection_options={"path": f"s3://{target_s3store}/output/", "partitionKeys": []},
    #transformation_ctx="target_s3",
)

target_Redshift = glueContext.write_dynamic_frame.from_options(
    frame=apply_Transformation,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": f"s3://{redshiftTmpDir_s3}/temporary/",
        "useConnectionProperties": "true",
        "dbtable": targer_redshift_dbtable,
        "connectionName": redshift_connection_name,
        "aws-iam-user" : target_iam_role_redshift
        #"preactions": "CREATE TABLE IF NOT EXISTS tbl_goals_redshift.table_goals_redshift (game_id BIGINT, no_of_minute INTEGER, player_name VARCHAR(255), team_1_score INTEGER, team_2_score INTEGER);",
    },
    # transformation_ctx="target_Redshift",
)

job.commit()
