import logging
import sys 
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import shutil

# basic syntax for logging module (logging module defines 5 standard severity levels)
# constant         numeric_value      meaning
# logging.DEBUG           10          detailed diagnostic info
# logging.INFO            20          general conformation that things are working
# logging.WARNING         30          something unexpected happened but not breaking
# logging.ERROR           40          something failed
# logging.CRITICAL        50          severe error




### path


BASE_DIR= Path(__file__).parent

RAW_DIR= BASE_DIR/"data"/"raw"
CLEANED_DIR= BASE_DIR/"data"/"cleaned"
TEMPLATE_DIR= BASE_DIR/"data"/"templates"
LOG_DIR= BASE_DIR/"logs"
ARCHIVE_DIR= BASE_DIR/"data"/"archive"
DUPLICATE_DIR= BASE_DIR/"data"/"duplicate"


### logger 

LOG_DIR.mkdir(exist_ok= True)  ## creates a new directory at the given path

def get_logger():
    logger= logging.getLogger("AutoETL")  ## returning a logger with a specified, creating one if it is necessary
    logger.setLevel(logging.INFO)   ## set the logging level of this logger
    
    if not logger.handlers:    ## decodes where the message goes
        formatter= logging.Formatter("%(asctime)s | [%(levelname)s] | %(message)s")  #initialize the formatter with specified format string
        file_handler= logging.FileHandler(LOG_DIR/"pipeline.log")  ## open the specified file and use it as the stream for logging
        file_handler.setFormatter(formatter)  ## set the formatter for this handler
        error_handler= logging.FileHandler(LOG_DIR/"error.log")
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        console_handler= logging.StreamHandler()  ## initailoze the handler
        console_handler.setFormatter(formatter)
        logger.addHandler(file_handler)   ## add the specified logger to this handler
        logger.addHandler(error_handler)
        logger.addHandler(console_handler)
        
    return logger
        
logger= get_logger()

## function for creating archive files
def archive_raw_file(file_path: Path) -> Path:
    ARCHIVE_DIR.mkdir(parents= True, exist_ok= True)
    timestamp= datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path= ARCHIVE_DIR/f"{file_path.stem}_{timestamp}{file_path.suffix}"
    shutil.copy2(file_path, archive_path)
    logger.info(f"archived raw file: {archive_path.name}")
    return archive_path
    
## ensuring duplicate folder exists
def ensure_duplicate_dir() -> None:
    DUPLICATE_DIR.mkdir(parents= True, exist_ok= True)
    
    
### configuration

## handling trailing commas
## pyhton's parser sees a comma followed by a ],) or } it simply recognizes the list/tuple/dict ends over here. 


DB_CONNECTION_STRING= (
    "mssql+pyodbc://@제니\\SQLEXPRESS/AutoETL"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
    "&Connection+Timeout=60"
)

etl_run_log_table= "dbo.etl_run_log"

TEMPLATE_CONFIG = {

    "APPLICABLE_DISCOUNT_IN": {

        "staging": "staging.applicable_discount",

        "main": "dbo.applicable_discount",
        
        "procedure": "staging.process_applicable_discount",

        "template_order": [
            "WEEK_DATE",
            "CHANNEL",
            "CHANNEL_TYPE",
            "PROD_LEVEL5_CODE",
            "PARTNER",
            "APPLICABLE_DISCOUNT_PERCENTAGE"
        ],

        "column_rules": {

            "date_cols": [
                "WEEK_DATE"
            ],

            "float_cols": [
                "APPLICABLE_DISCOUNT_PERCENTAGE"
            ],

            "text_cols": [
                "CHANNEL",
                "CHANNEL_TYPE",
                "PROD_LEVEL5_CODE",
                "PARTNER"
            ],

            "required_columns": [
                "WEEK_DATE",
                "CHANNEL",
                "CHANNEL_TYPE",
                "PROD_LEVEL5_CODE",
                "PARTNER",
                "APPLICABLE_DISCOUNT_PERCENTAGE"
            ]
        }
    },

    "ASSORTMENT_PLAN_IN": {

        "staging": "staging.assortment_plan",

        "main": "dbo.assortment_plan",
        
        "procedure": "staging.process_assortment_plan",

        "template_order": [
            "WEEK_DATE",
            "CHANNEL",
            "PARTNER",
            "CHANNEL_TYPE",
            "OPTION_CODE",
            "STORE_CODE",
            "GENDER",
            "LISTING_STATUS"
        ],

        "column_rules": {
            "date_cols": [
                "WEEK_DATE"
            ],

            "text_cols": [
                "CHANNEL",
                "PARTNER",
                "CHANNEL_TYPE",
                "OPTION_CODE",
                "STORE_CODE",
                "GENDER",
                "LISTING_STATUS"
            ],

            "required_columns": [
                 "WEEK_DATE",
                "CHANNEL",
                "PARTNER",
                "CHANNEL_TYPE",
                "OPTION_CODE",
                "STORE_CODE",
                "GENDER",
                "LISTING_STATUS"
            ]
        }
    },

    "DISCOUNT_BUCKET_IN": {

        "staging": "staging.discount_bucket",

        "main": "dbo.discount_bucket",
        
        "procedure": "staging.process_discount_bucket",

        "template_order": [
            "WEEK_DATE",
            "CHANNEL_TYPE",
            "CHANNEL",
            "DEPARTMENT",
            "GENDER",
            "PARTNER",
            "MIN_DISCOUNT_RANGE",
            "MAX_DISCOUNT_RANGE",
        ],

        "column_rules": {
            "date_cols": [
                "WEEK_DATE"
            ],

            "int_cols": [
                "MIN_DISCOUNT_RANGE",
                "MAX_DISCOUNT_RANGE"
            ],

            "text_cols": [
                "GENDER",
                "CHANNEL_TYPE",
                "CHANNEL",
                "DEPARTMENT",
                "PARTNER"
            ],

            "required_columns": [
                "WEEK_DATE",
                "GENDER",
                "CHANNEL_TYPE",
                "CHANNEL",
                "DEPARTMENT",
                "PARTNER",
                "MIN_DISCOUNT_RANGE",
                "MAX_DISCOUNT_RANGE"
            ]
        }
    },

    "DISCOUNT_BUDGET_IN": {

        "staging": "staging.discount_budget",

        "main": "dbo.discount_budget",
        
        "procedure": "staging.process_discount_budget",

        "template_order": [
            "WEEK_DATE",
            "GENDER",
            "CHANNEL_TYPE",
            "CHANNEL",
            "PARTNER",
            "DISCOUNT_BUDGET_VAL"
        ],

        "column_rules": {
            "date_cols": [
                "WEEK_DATE"
            ],

            "int_cols": [
                "DISCOUNT_BUDGET_VAL"
            ],

            "text_cols": [
                "GENDER",
                "CHANNEL_TYPE",
                "CHANNEL",
                "PARTNER"
            ],

            "required_columns": [
                "WEEK_DATE",
                "GENDER",
                "CHANNEL_TYPE",
                "CHANNEL",
                "PARTNER",
                "DISCOUNT_BUDGET_VAL"
            ]
        }
    },

    "EVENT_DISCOUNT_BUCKET_IN": {

        "staging": "staging.event_discount_bucket",

        "main": "dbo.event_discount_bucket",
        
        "procedure": "staging.process_event_discount_bucket",

        "template_order": [
            "EVENT",
            "EVENT_TYPE",
            "EVENT_PRIORITY",
            "CHANNEL_TYPE",
            "DEPARTMENT",
            "SUB_CATEGORY",
            "GENDER",
            "STYLE_CODE",
            "OPTION_CODE",
            "DISCOUNT_TYPE",
            "MIN_DISCOUNT_PERCENT",
            "MAX_DISCOUNT_PERCENT",
            "LIQUIDATION_QTY",
            "LIQUIDATION_PERCENT",
            "PARTNER"
        ],

        "column_rules": {
            "int_cols": [
                "EVENT_PRIORITY"
            ],

            "float_cols": [
                "MIN_DISCOUNT_PERCENT",
                "MAX_DISCOUNT_PERCENT",
                "LIQUIDATION_PERCENT",
                "LIQUIDATION_QTY"
            ],

            "text_cols": [
                "EVENT",
                "EVENT_TYPE",
                "CHANNEL_TYPE",
                "DEPARTMENT",
                "SUB_CATEGORY",
                "GENDER",
                "STYLE_CODE",
                "OPTION_CODE",
                "DISCOUNT_TYPE",
                "PARTNER"
            ],

            "required_columns": [
                "EVENT",
                "EVENT_TYPE",
                "EVENT_PRIORITY",
                "CHANNEL_TYPE",
                "DEPARTMENT",
                "SUB_CATEGORY",
                "GENDER",
                "STYLE_CODE",
                "OPTION_CODE",
                "DISCOUNT_TYPE",
                "MIN_DISCOUNT_PERCENT",
                "MAX_DISCOUNT_PERCENT",
                "LIQUIDATION_QTY",
                "LIQUIDATION_PERCENT",
                "PARTNER"
            ]
        }
    },

    "STYLE_AGEING_IN": {

        "staging": "staging.style_ageing",

        "main": "dbo.style_ageing",
        
        "procedure": "staging.process_style_ageing",

        "template_order": [
            "MONTH_DATE",
            "CHANNEL",
            "PARTNER",
            "CHANNEL_TYPE",
            "OPTION_CODE",
            "MIN_DISCOUNT_RANGE",
            "MAX_DISCOUNT_RANGE",
            "AGE"
        ],

        "column_rules": {
            "date_cols": [
                "MONTH_DATE"
            ],

            "float_cols": [
                "MIN_DISCOUNT_RANGE",
                "MAX_DISCOUNT_RANGE",
            ],

            "text_cols": [
                "CHANNEL",
                "PARTNER",
                "CHANNEL_TYPE",
                "OPTION_CODE",
                "AGE"
            ],

            "required_columns": [
                "MONTH_DATE",
                "CHANNEL",
                "PARTNER",
                "CHANNEL_TYPE",
                "OPTION_CODE",
                "MIN_DISCOUNT_RANGE",
                "MAX_DISCOUNT_RANGE",
                "AGE"
            ]
        }
    }
}

### ingest 

class IngestError(Exception):
    pass
    
def read_raw_file(file_path: Path) -> pd.DataFrame:  ## the parameter over here is file_path, 
    ## with a path hint saying that it should be a Path object
    ## and a return type hint saying this function will return a pandas dataframe
    try:
        suffix= file_path.suffix.lower()   ## extracts a file extension (.csv, .xlsx, etc.) and normalizes it to lowercase
        
        if suffix == ".csv":
            return pd.read_csv(file_path)
        if suffix in [".xlsx", ".xls"]:
            return pd.read_excel(file_path)
        if suffix == ".xlsb":
            return pd.read_excel(file_path, engine= "pyxlsb")
        raise IngestError(f"unsupported file format: {suffix}")
        
    except Exception as e:
        raise IngestError(str(e))
        
 
### template_detector

class TemplateDetectionError(Exception):
    pass
 
## reads the header row of the csv template and return the column names as a cleaned up, normalized "set" of strings 
## df.column= gives you the column name of the dataframe as a pandas index object
def get_template_headers(template_file: Path) -> set:
    df= pd.read_csv(template_file, nrows= 0)
    return {str(col).strip().upper() for col in df.columns}

## calculates what fraction of expected template columns are actually present in an uploaded file
## essentially a similarity match score between 0.0(no match) and 1.0(perfect match).
def calculate_match_score(file_columns: set, template_columns: set) -> float:
    if not template_columns:
        return 0.0  ## is template_columns is an empty set it will return 0
    matches= len(file_columns.intersection(template_columns))    
    ## intersection is built in set method that returns a new set containing elements present in both set
    return matches/len(template_columns)

# ties together both the previous function together
def detect_template(df: pd.DataFrame, threshold= 0.8) -> tuple[str, float]:  
    ## type hint returns a tuple of 2 values a string and a float
    file_columns={str(col).strip().upper() for col in df.columns}
    best_template= None
    best_score= 0
    
    for template_file in TEMPLATE_DIR.glob("*.csv"):  ## finding files matching a pattern
        template_columns= get_template_headers(template_file)
        score= calculate_match_score(file_columns, template_columns)
        
        if score > best_score:
            best_score= score
            best_template= template_file.stem  ## gives file name 
        
    if best_template is None:
        raise TemplateDetectionError("no matching template found.")
          
    if best_score < threshold:
        raise TemplateDetectionError(f"no template matched above {threshold:.0%} best match: {best_template} ({best_score:.2%})")
    
    return best_template, best_score
    
### clean

class CleaningError(Exception):
    pass

## this function takes a df and returns a new df with cleaned up columns names  
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df= df.copy()
    df.columns=[str(col).strip().upper() for col in df.columns]
    return df

## core data cleaning
def clean_and_standardize(df: pd.DataFrame, template_name: str) -> pd.DataFrame:
    try:
        df= standardize_columns(df)
        rules= TEMPLATE_CONFIG[template_name]["column_rules"]
        
        # validate required columns
        missing= [col for col in rules.get("required_columns", []) if col not in df.columns]
        
        if missing:
            raise CleaningError(f"missing required columns: {missing}")
            
        #date columns
        for col in rules.get("date_cols", []):  
            ## dictionary method that safely retrieves a value by key, w/o raising an error if the key doesnt exists.
            if col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):  
                    ## checks whether the column holds the numeric data or not 
                    df[col]= pd.to_datetime(df[col], unit= "D", origin= "1899-12-30", errors= "coerce")
                    ## .to_datetime converts values into proper datetime objects
                else:
                    ## else normal loop
                    df[col]= pd.to_datetime(df[col], errors= "coerce")
                df[col]= df[col].dt.strftime("%Y-%m-%d")
                ## takes a real datetime column and formats it back it into a string in specified pattern
        # float columns
        for col in rules.get("float_cols", []):
            if col in df.columns:
                df[col]= pd.to_numeric(df[col], errors= "coerce").astype(float)
                
        # integer columns
        for col in rules.get("int_cols", []):
            if col in df.columns:
                df[col]= pd.to_numeric(df[col], errors= "coerce").astype("Int64")
                
        # text columns
        for col in rules.get("text_cols", []):
            if col in df.columns:
                df[col]= (df[col].astype("string").str.strip().str.upper())                
        
        return df
        
    except Exception as e:
        raise CleaningError(str(e))
        
def save_cleaned(df: pd.DataFrame, original_name: str) -> str:
    CLEANED_DIR.mkdir(parents= True, exist_ok= True)
    out_path= (CLEANED_DIR /f"{Path(original_name).stem}_cleaned.csv")
    df.to_csv(out_path, index= False)
    return str(out_path)
    
### mapper

class TransformerError(Exception):
    pass
    
def map_to_template(df: pd.DataFrame, template_name: str) -> pd.DataFrame:
    try:
        ordered_columns= TEMPLATE_CONFIG[template_name]["template_order"]
        missing= [col for col in ordered_columns if col not in df.columns]
        if missing:
            raise TransformerError(f"missing template columns: {missing}")
            
        return df[ordered_columns].copy()
        
    except Exception as e:
        raise TransformerError(str(e))
        
### database connector

class DatabaseError(Exception):
    pass
    
def get_engine():
    try:
        return create_engine(DB_CONNECTION_STRING)
        ## imports from sqlalchemy and helps builds connection pointed at your database
    except Exception as e:
        raise DatabaseError(str(e))
        
def load_to_staging(df, engine, staging_table: str) -> None:
    try:
        logger.info(f"loading {len(df)} rows into {staging_table}")
        schema, table= (staging_table.split(".") if "." in staging_table else (None, staging_table))
        with engine.begin() as conn:
            conn.execute(text(f"truncate table {staging_table}"))
        df.to_sql(table, engine, schema= schema, if_exists= "append", index= False, method= "multi", chunksize= 100)
        
    except Exception as e:
        raise DatabaseError(str(e))
        
def run_sql_processing(engine, procedure_name: str) -> None:
    try:
        logger.info(f"running sql procedure: {procedure_name}")
        with engine.connect().execution_options(isolation_level= "AUTOCOMMIT") as conn:
            conn.execute(text(f"EXEC {procedure_name}"))
        logger.info(f"sql procedure completed: {procedure_name}")
        
    except Exception as  e:
        raise DatabaseError(str(e))
        

def log_run(engine, file_name: str, template_name, rows_loaded: int, status: str, error_message, started_at: datetime) -> None:
    finished_at= datetime.now()
    duration_seconds= (finished_at - started_at).total_seconds()

    try:
        with engine.begin() as conn:
            conn.execute(
    text("""
        INSERT INTO dbo.etl_run_log
        (
            file_name,
            template_name,
            rows_loaded,
            status,
            error_message
        )
        VALUES
        (
            :file_name,
            :template_name,
            :rows_loaded,
            :status,
            :error_message
        )
    """),
    {
        "file_name": file_name,
        "template_name": template_name,
        "rows_loaded": rows_loaded,
        "status": status,
        "error_message": error_message,
    }
)
    except Exception:
        logger.exception("failed to write run record to dbo.etl_run_log")


def run_pipeline(file_path: str) -> dict:
    file_path= Path(file_path)
    started_at= datetime.now()
    template_name= None
    rows_loaded= 0
    engine= None

    try:
        engine= get_engine()
        archive_raw_file(file_path)
        ensure_duplicate_dir()
        raw_df= read_raw_file(file_path)
        template_name, score= detect_template(raw_df)
        logger.info(f"detected template: {template_name} ({score:.2%} match)")
        template_config= TEMPLATE_CONFIG[template_name]
        clean_df= clean_and_standardize(raw_df, template_name)
        save_cleaned(clean_df, file_path.name)
        template_df= map_to_template(clean_df, template_name)
        rows_loaded= len(template_df)
        load_to_staging(template_df, engine, template_config["staging"])
        procedure_name= template_config.get("procedure")
        if procedure_name:
            run_sql_processing(engine, procedure_name)
        logger.info(f"pipeline completed successfully for {template_name}")

        log_run(engine, file_path.name, template_name, rows_loaded,
                "SUCCESS", None, started_at)

        return {
            "status": "SUCCESS",
            "file_name": file_path.name,
            "template": template_name,
            "rows_loaded": rows_loaded,
        }

    except Exception as e:
        logger.error(f"File={file_path.name} | Template={template_name} | Error={e}")
        logger.exception("unexpected error during pipeline run")

        if engine is not None:
            log_run(engine, file_path.name, template_name, rows_loaded,
                    "FAILED", str(e), started_at)

        return {
            "status": "FAILED",
            "file_name": file_path.name,
            "template": template_name,
            "error": str(e),
        }
    
### calling it

def get_raw_files() -> list[Path]:
    files= []
    files.extend(RAW_DIR.glob("*.csv"))
    files.extend(RAW_DIR.glob("*.xlsx"))
    files.extend(RAW_DIR.glob("*.xls"))
    files.extend(RAW_DIR.glob("*.xlsb"))
    return files
    
def run_scheduled_pipeline() -> None:
    logger.info("scheduleed etl started")
    RAW_DIR.mkdir(parents= True, exist_ok= True)
    files= get_raw_files()
    if not files:
        logger.info("no raw files found.")
        return 
    for file_path in files:
        result= run_pipeline(str(file_path))
        logger.info(f"results: {result}")
    logger.info("scheduled etl completed.")
### main calling fucntion
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_pipeline(sys.argv[1])
    else:
        run_scheduled_pipeline()

