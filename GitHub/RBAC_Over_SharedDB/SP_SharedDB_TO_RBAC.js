use role accountadmin;

create database if not exists admin;
create schema if not exists admin.storedprocs;
use admin.storedprocs;

drop database "SNOWFLAKE_RBAC";

create or replace procedure admin.storedprocs.sp_sharedDB_For_RBAC(SOURCEDB string, WRAPPERDB string)
returns variant null
language javascript
strict
as
$$
    var retVal = {};
    retVal.output = {};
    retVal.Database = [];
    retVal.Schemas = [];
    retVal.Views = [];
    retVal.Tables = [];
    
    //Certain columnNames MUST be addressed in double quotes
    var doubleQuoteKeywords = ['ROWS', 'INCREMENT'];

    //---------------------
    //-- CREATE DATABASE --
    //---------------------
    //Create the wrapperDB to hold the objects for RBAC
    var createDB_query = `create or replace database ${WRAPPERDB};`;
    retVal.Database.push(createDB_query);
    snowflake.execute({sqlText: createDB_query})
    
    
    //--------------------
    //-- CREATE SCHEMAS --
    //--------------------
    try
    {
        //Create the wrapperDB to hold the objects for RBAC
        var schemaNames_query = `show schemas in database ${SOURCEDB};`;
        var schemaNames_queryid = snowflake.execute({sqlText: schemaNames_query}).getQueryId();

        //create the schemas
        var schemaNames_query = `select distinct "name"
                                from table(result_scan('${schemaNames_queryid}'))
                                where "name" not in ('INFORMATION_SCHEMA', 'PUBLIC');`

        var schemaNames_results = snowflake.execute({sqlText: schemaNames_query});

        while(schemaNames_results.next()) {
            var schemaName = schemaNames_results.getColumnValue(1);
            var createSchema_query = `create schema ${WRAPPERDB}.${schemaName};`;

            retVal.Schemas.push(createSchema_query);
            snowflake.execute({sqlText: createSchema_query});
        }
    }
    catch (err)
    {
        retVal.status = 'errored - schemas';
        retVal.output = err;
    }
        
        
        
    //-----------
    //-- VIEWS --
    //-----------
    try
    {
        //Get all views in the database
        var showViews_query = `show views in database ${SOURCEDB};`;
        var showViews_results = snowflake.execute({sqlText: showViews_query});

        //Loop through the views
        while(showViews_results.next()) {
            var schemaName = showViews_results.getColumnValue(5);
            var viewName = showViews_results.getColumnValue(2);
            
            //Don't touch the Information_schema
            if (schemaName === 'INFORMATION_SCHEMA')
                { continue; }

            //Get all columns in the view
            var showViewColumns_query = `show columns in view ${SOURCEDB}.${schemaName}.${viewName};`;
            var showViewColumns_results = snowflake.execute({sqlText: showViewColumns_query});

            //prepare the CreateView statement
            var createView_query = `create or replace view ${WRAPPERDB}.${schemaName}.${viewName}
                                    as
                                    select `;

            //Loop through the columns, append to the createView statement
            while(showViewColumns_results.next()) {
                colName = showViewColumns_results.getColumnValue(3);
                
                //Anything in list doubleQuoteKeywords MUST have double quotes to be used as a column name
                (doubleQuoteKeywords.includes(colName))
                ? createView_query += `"${colName}", `
                : createView_query += `${colName}, `
            }

            //after the last column, theres a trailing ", " to be removed
            createView_query = createView_query.slice(0, -2); 

            //add in the FROM (table/view)
            createView_query += ` from ${SOURCEDB}.${schemaName}.${viewName}`;

            //Finally - create the view!    
            retVal.Views.push(createView_query);
            snowflake.execute({sqlText: createView_query});
        }
    }
    catch (err)
    {
        retVal.status = 'errored - views';
        retVal.output = err;
    }
    

    //------------
    //-- TABLES --
    //------------
    try
    {
        //Get all views in the database
        var showTables_query = `show tables in database ${SOURCEDB};`;
        var showTables_results = snowflake.execute({sqlText: showTables_query});

        //Loop through the views
        while(showTables_results.next()) {
            var schemaName = showTables_results.getColumnValue(4);
            var tableName = showTables_results.getColumnValue(2);
            
            //Don't touch the Information_schema
            if (schemaName == 'INFORMATION_SCHEMA')
                { continue; }

            //Get all columns in the view
            var showTableColumns_query = `show columns in table ${SOURCEDB}.${schemaName}.${tableName};`;
            var showTableColumns_results = snowflake.execute({sqlText: showTableColumns_query});

            //prepare the CreateView statement
            var createView_query = `create or replace view ${WRAPPERDB}.${schemaName}.vw_${tableName}
                                    as
                                    select `;

            //Loop through the columns, append to the createView statement
            while(showTableColumns_results.next()) {
                colName = showTableColumns_results.getColumnValue(3);
                
                //Anything in list doubleQuoteKeywords MUST have double quotes to be used as a column name
                (doubleQuoteKeywords.includes(colName))
                    ? createView_query += `"${colName}", `
                    : createView_query += `${colName}, `
            }

            //after the last column, theres a trailing ", " to be removed
            createView_query = createView_query.slice(0, -2); 

            //add in the FROM (table/view)
            createView_query += ` from ${SOURCEDB}.${schemaName}.${tableName}`;

            //Finally - create the view!    
            retVal.Tables.push(createView_query);
            snowflake.execute({sqlText: createView_query});
        }
    }
    catch (err)
    {
        retVal.status = 'errored - tables';
        retVal.output = err;
    }
    
    return retVal;
$$;


--Test it out. This database only has views
set sourcedb = 'SNOWFLAKE';
set wrapperdb  = 'SNOWFLAKE_RBAC';
call admin.storedprocs.sp_sharedDB_For_RBAC($sourcedb, $wrapperdb);

----Test it out. This database only has tables
--Tables will be prefixed with "vw_"
set sourcedb = 'SNOWFLAKE_SAMPLE_DATA';
set wrapperdb  = 'SNOWFLAKE_SAMPLE_DATA_RBAC';
call admin.storedprocs.sp_sharedDB_For_RBAC($sourcedb, $wrapperdb);


//Get the individual create scripts
//select  SP_SHAREDDB_FOR_RBAC:Database::string as Database_Script
//from table(result_scan(last_query_id()))
select f.Value::string as Database_Script
from table(result_scan(last_query_id())), 
    lateral flatten (input => SP_SHAREDDB_FOR_RBAC:Database) f

union

select f.Value::string as Schema_Scripts
from table(result_scan(last_query_id())), 
    lateral flatten (input => SP_SHAREDDB_FOR_RBAC:Schemas) f

union

select f.Value::string as TablesAsViews_Scripts
from table(result_scan(last_query_id())), 
    lateral flatten (input => SP_SHAREDDB_FOR_RBAC:Tables) f

union 

select f.Value::string as View_Scripts
from table(result_scan(last_query_id())), 
    lateral flatten (input => SP_SHAREDDB_FOR_RBAC:Views) f
;