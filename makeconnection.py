import sys
from snowflake.snowpark import Session

# If you are running this code in Github code spaces you don't need to execute below command,
#sys.path.append('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_pipeline/')

from generic_code import code_library

# Make connection and create Snowpark session.
# Please mention your snowflake account credentials below,
connection_parameters = {"account":"dbobrgv-cfa87896", \
"user":"AVERNEKAR", \
"password": "Gspot@123", \
"role":"ACCOUNTADMIN", \
"warehouse":"COMPUTE_WH", \
"database":"DEMO_DB", \
"schema":"PUBLIC" \
}

# Create connection with snowflake and return the session
session = code_library.snowconnection(connection_parameters)

session = Session.builder.configs(connection_parameters).create()
session.session_id
current_user = session.sql("select current_user()").collect()
current_user[0][0]

session.get_current_warehouse()
session.get_current_role()
