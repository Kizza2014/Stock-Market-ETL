import pandas as pd
from dotenv import load_dotenv
import os
import pyodbc
from utils.timeutils import convert_date_to_int, get_day_of_week, is_valid_datetime_format, get_quarter, is_weekend


def establish_dw_connection():
    load_dotenv()
    DRIVER = os.getenv("SQL_DRIVER")
    SERVER = os.getenv("SQL_SERVER")
    DATA_WAREHOUSE = os.getenv("DATA_WAREHOUSE")
    LOGIN = os.getenv("LOGIN")
    PASSWORD = os.getenv("PASSWORD")
    conn = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATA_WAREHOUSE};UID={LOGIN};PWD={PASSWORD}')
    
    return conn


# transforms
def process_marital_status(value):
    MARITAL_STATUS = ["Single", "Married", "Widowed", "Other"]
    if pd.isna(value):
        return "Other"
    
    # datetime means wedding day -> married
    if is_valid_datetime_format(value):
        return "Married"
    if not value in MARITAL_STATUS:
        return "Other"
    return value


def process_gender(value):
    GENDER_VALUES = ["Male", "Female", "Other"]
    if not value in GENDER_VALUES:
        return "Other"
    return value


def process_language(value):
    LANGUAGE_VALUES = ['ENGLISH', 'SPANISH', 'FRENCH', 'POLISH', 'ITALIAN', 'BENGALI', 'OTHER', 'DARI', 'GREEK',
                        'SIGN LANGUAGE', 'MANDARIN', 'VIETNAMESE', 'PORTUGUESE']
    if not value in LANGUAGE_VALUES:
        return "OTHER"
    return value


def process_ethnicity(value):
    actual_value = value.split(",")[-1] # remove leading numbers
    ETHNICITY_VALUES = ['Hispanic or Latino', 'White', 'Black or African American', 'Asian or Pacific Islander', 'Unknown']
    if not actual_value in ETHNICITY_VALUES:
        return "Unknown"
    return value


def process_dim_patients(filepath):
    # import data
    selected_columns = ["Master Patient ID", "Patient Name", "Patient City", "Patient County", "Patient State", "Patient Country", 
                        "Patient Gender", "Patient Marital Status", "Patient Primary Language", "Patient Ethnicity"]
    df = pd.read_csv(filepath)[selected_columns]

    # process attributes
    df["Patient Marital Status"] = df["Patient Marital Status"].apply(process_marital_status)
    df["Patient Gender"] = df["Patient Gender"].apply(process_gender)
    df["Patient Primary Language"] = df["Patient Primary Language"].apply(process_language)
    df["Patient Ethnicity"] = df["Patient Ethnicity"].apply(process_ethnicity)
    print(f"Processed {df.shape[0]} records for dim_Patient.")

    # load to data warehouse
    conn = establish_dw_connection()
    cursor = conn.cursor()
    QUERY = """INSERT INTO dim_Patient ([Patient ID], [Patient Name], [Patient City], [Patient County], [Patient State], 
                        [Patient Country], [Patient Gender], [Patient Marital Status], [Patient Primary Language], [Patient Ethnicity])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
    for _, row in df.iterrows():
        cursor.execute(QUERY, *row)
    cursor.commit()
    print(f"Loaded {df.shape[0]} records into dim_Patient")


def process_dim_surgeon(filepath):
    # import data
    df = pd.read_csv(filepath)
    columns_order = ["Provider ID", "Provider First Name", "Provider Last Name", "Provider Full Name"]
    df = df[columns_order]

    # load to data warehouse
    conn = establish_dw_connection()
    cursor = conn.cursor()
    QUERY = """
        INSERT INTO dim_Surgeon ([Surgeon ID], [Surgeon First Name], [Surgeon Last Name], [Surgeon Full Name])
        VALUES (?, ?, ?, ?)
    """
    for _, row in df.iterrows():
        cursor.execute(QUERY, *row)
    cursor.commit()
    print(f"Loaded {df.shape[0]} records into dim_Surgeon")


# def process_dim_surgical_detail(filepath):
#     # import data
#     df = pd.read_csv(filepath)


def process_dim_admission_date(filepath):
    # import data
    df = pd.read_csv(filepath)
    admission_date = df[["Surgical Admission Date"]]

    # process datetime
    admission_date["Admission Date ID"] = admission_date["Surgical Admission Date"].apply(convert_date_to_int) 
    admission_date["Day of Month"] = admission_date["Surgical Admission Date"].apply(lambda value: value.split("-")[-1])
    admission_date["Day of Week"] = admission_date["Surgical Admission Date"].apply(get_day_of_week)
    admission_date["Month"] = admission_date["Surgical Admission Date"].apply(lambda value: value.split("-")[1])
    admission_date["Year"] = admission_date["Surgical Admission Date"].apply(lambda value: value.split("-")[0])
    admission_date["Quarter"] = admission_date["Surgical Admission Date"].apply(get_quarter)
    admission_date["Is Weekend"] = admission_date["Surgical Admission Date"].apply(is_weekend)

    # drop column and duplicates
    admission_date.drop(["Surgical Admission Date"], axis=1, inplace=True)
    admission_date.drop_duplicates(inplace=True)
    print(f"Processed {admission_date.shape[0]} records for dim_AdmissionDate")

    # load to data warehouse
    conn = establish_dw_connection()
    cursor = conn.cursor()
    QUERY = """
        INSERT INTO dim_AdmissionDate ([Admission Date ID], [Day of Month], [Day of Week], [Month], [Year], [Quarter], [Is Weekend])
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    for _, row in admission_date.iterrows():
        cursor.execute(QUERY, *row)
    cursor.commit()
    print(f"Loaded {admission_date.shape[0]} records to dim_AdmissionDate")


def process_dim_discharge_date(filepath):
    # import data
    df = pd.read_csv(filepath)
    admission_date = df[["Surgical Discharge Date"]]

    # process datetime
    admission_date["Discharge Date ID"] = admission_date["Surgical Discharge Date"].apply(convert_date_to_int) 
    admission_date["Day of Month"] = admission_date["Surgical Discharge Date"].apply(lambda value: value.split("-")[-1])
    admission_date["Day of Week"] = admission_date["Surgical Discharge Date"].apply(get_day_of_week)
    admission_date["Month"] = admission_date["Surgical Discharge Date"].apply(lambda value: value.split("-")[1])
    admission_date["Year"] = admission_date["Surgical Discharge Date"].apply(lambda value: value.split("-")[0])
    admission_date["Quarter"] = admission_date["Surgical Discharge Date"].apply(get_quarter)
    admission_date["Is Weekend"] = admission_date["Surgical Discharge Date"].apply(is_weekend)

    # drop column and duplicates
    admission_date.drop(["Surgical Discharge Date"], axis=1, inplace=True)
    admission_date.drop_duplicates(inplace=True)
    print(f"Processed {admission_date.shape[0]} records for dim_AdmissionDate")

    # load to data warehouse
    conn = establish_dw_connection()
    cursor = conn.cursor()
    QUERY = """
        INSERT INTO dim_DischargeDate ([Discharge Date ID], [Day of Month], [Day of Week], [Month], [Year], [Quarter], [Is Weekend])
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    for _, row in admission_date.iterrows():
        cursor.execute(QUERY, *row)
    cursor.commit()
    print(f"Loaded {admission_date.shape[0]} records to dim_DischargeDate")


def process_fact(filepath):
    # import data
    df = pd.read_csv(filepath)
    selected_columns = ["Surgery ID", "Master Patient ID", "Surgeon ID", "Surgical Admission Date", "Surgical Discharge Date",
                        "Surgical Total Cost", "Surgical Total Profit", "Surgical LOS"]
    df = df[selected_columns]
    df.rename(columns={"Surgical Total Cost": "Total Cost", "Surgical Total Profit": "Total Profit", "Master Patient ID": "Patient ID", 
               "Surgical LOS": "LOS"}, inplace=True)
    
    # process dim key
    df["Admission Date ID"] = df["Surgical Admission Date"].apply(convert_date_to_int)
    df["Discharge Date ID"] = df["Surgical Discharge Date"].apply(convert_date_to_int)
    df["Surgical Detail ID"] = 1  # dump values, fix later

    # remove records with no patient id
    df.dropna(subset=["Patient ID"], axis=0, inplace=True)
    df["Patient ID"] = df["Patient ID"].astype(int)

    # drop columns
    df.drop(["Surgical Admission Date", "Surgical Discharge Date"], axis=1, inplace=True)
    columns_order = ["Surgery ID", "Patient ID", "Surgeon ID", "Admission Date ID", "Discharge Date ID", "Surgical Detail ID",
                     "Total Cost", "Total Profit", "LOS"]
    df = df[columns_order]
    print(f"Processed {df.shape[0]} records for fact_SurgicalMeasurement")

    # load to data warehouse
    conn = establish_dw_connection()
    cursor = conn.cursor()
    QUERY = """
        INSERT INTO fact_SurgicalMeasurement ([Surgical ID], [Patient ID], [Surgeon ID], [Admission Date ID], [Discharge Date ID],
                                                [Surgical Detail ID], [Total Cost], [Total Profit], [LOS])
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for _, row in df.iterrows():
        cursor.execute(QUERY, *row)
    cursor.commit()
    print(f"Loaded {df.shape[0]} records into fact_SurgicalMeasurement")


if __name__ == "__main__":
    # process_dim_patients("staging/Patients_2025_07_03.csv")
    # process_dim_surgeon("staging/Physicians_2025_07_03.csv")
    # process_dim_admission_date("staging/SurgicalEncounters_2025_07_03.csv")
    # process_dim_discharge_date("staging/SurgicalEncounters_2025_07_03.csv")
    process_fact("staging/SurgicalEncounters_2025_07_03.csv")
