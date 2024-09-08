# Below code is part of Lecture,

# Create snowconnction component part 1
#https://www.udemy.com/course/snowpark-data-engineering-with-snowflake/learn/lecture/35877124#overview

# Create snowconnection component part 2
# https://www.udemy.com/course/snowpark-data-engineering-with-snowflake/learn/lecture/35877126#overview

# Copy to snowflake table.
#https://www.udemy.com/course/snowpark-data-engineering-with-snowflake/learn/lecture/36039456?instructorPreviewMode=instructor_v4#overview


import sys
sys.path.append('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_pipeline/')
from generic_code import code_library
from schema import src_stg_schema
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import IntegerType, StringType,StructType,DateType,StructField
import json

config_snow_copy = open('./config/copy_to_snowstg.json', "r")
config_snow_copy = json.loads(config_snow_copy.read())
print(config_snow_copy)

connection_parameter = open('./config/connection_details.json', "r")
connection_parameter = json.loads(connection_parameter.read())


session = code_library.snowconnection(connection_parameters)

copied_into_result, qid = code_library.copy_to_table(session,config_file,schema)

print(copied_into_result)
print(qid)

copied_into_result_df = session.create_dataframe(copied_into_result)
copied_into_result_df.show()