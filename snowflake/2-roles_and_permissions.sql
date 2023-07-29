-- Set up roles and warehouse permissions
use role securityadmin;
create role loader;
grant all on warehouse loading to role loader; 
create role transformer;
grant all on warehouse transforming to role transformer;
create role reporter;
grant all on warehouse reporting to role reporter;

-- Create users and assigning them to their roles
create user '<user_name>'
    password = '_generate_this_'
    default_warehouse = transforming
    default_role = transformer
    must_change_password = true;

create user dbt_user
    password = '_generate_this_'
    default_warehouse = transforming
    default_role = transformer;

create user tableau_user
    password = '_generate_this_'
    default_warehouse = reporting
    default_role = reporter;

-- Grant these roles to each user
grant role transformer to user '<user_name>';
grant role transformer to user dbt_user; 
grant role reporter to user tableau_user;

-- The role LOADER can access just the RAW schema.
grant usage on database bi_engineer_challenger to role loader;
grant all privileges on schema bi_engineer_challenger.raw to role loader;
revoke all privileges on schema bi_engineer_challenger.staging from role loader;
revoke all privileges on schema bi_engineer_challenger.analytics from role loader;

-- The role TRANSFORMER can only read the RAW schema but must have read and write access to the STAGING and ANALYTICS schemas.
grant usage on database bi_engineer_challenger to role transformer;
grant select on all tables in schema bi_engineer_challenger.raw to role transformer;
grant all privileges on schema bi_engineer_challenger.staging to role transformer;
grant all privileges on schema bi_engineer_challenger.analytics to role transformer;

-- The role REPORTER can acces just read the ANALYTICS schema
grant usage on database bi_engineer_challenger to role reporter;
revoke all privileges on schema bi_engineer_challenger.raw from role reporter;
revoke all privileges on schema bi_engineer_challenger.staging from role reporter;
grant select on all tables in schema bi_engineer_challenger.analytics to role reporter;
