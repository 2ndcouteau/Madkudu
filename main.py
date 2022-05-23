#!/usr/bin/env python3

import io
import s3fs
import click
from halo import Halo

import pandas
import sqlite3
import sys
import traceback

from datetime import datetime
import dateutil.relativedelta

current_time = datetime.now() - dateutil.relativedelta.relativedelta(months=1)

# import pdb
# pdb.set_trace()


@click.group()
def main():
    """
    CLI to download CSV files from s3 and load them in an sqlite DB
    """
    pass


@main.command()
@click.option('--year', '-y', prompt='Select the year', prompt_required=False, default=f"{current_time.year}", help=f"Format YYYY::{current_time.year}")
@click.option('--month', '-m', prompt='Select the month', prompt_required=False, default=f"{current_time.month:02}", help=f"Format MM::{current_time.month:02}")
@click.option('--host', '-h', default="s3://work-sample-mk", help="S3 path::s3://work-sample-mk")
@click.option('--filename', '-f', default="events.csv", help="File name::events.csv")
@click.option('--dbname', default="Madkudu_events.db", help="Chose the destination DB::Madkudu_events.db")
@click.option('--clean', '-c', is_flag=True, help="Clean the data")
@click.option('--debug', '-d', is_flag=True, help="Show error details")
def loadFile(year: str, month: str, host: str, filename: str, dbname: str, clean :bool, debug :bool):
    """Download file and load it in the sqlite DB\n
    --year, --month can be passed without value. You will get a prompt which invit you to pass your value.
    If these options are not passed, the default value will be the last month of the current year.\n
    You can decide to clean the data thanks to the --clean option\n
    This will aggregate the duplicated rows and add a column 'count' with the total count for each rows"""

    try:
        # Init connection with the DB and create Tables if not exist yet
        conn = sqlite3.connect(dbname)
        cursor = conn.cursor()
        print("Connection to the DB established")

        cursor.execute("CREATE TABLE IF NOT EXISTS Events (id TEXT,timestamp DATE, email TEXT, country TEXT,ip TEXT, uri TEXT, action TEXT, tags TEXT, count INT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS Files (name TEXT PRIMARY KEY NOT NULL)")
      
        # Check if the file has already been added in the DB
        notNewFile = cursor.execute("SELECT name FROM Files WHERE name=?", (f"{host}/{year}/{month}/{filename}",)).fetchone()
        if notNewFile:
            print(f"The file '../{year}/{month}/{filename}' has already been inserted in the DB")
        else:
            # Create S3 Object to connect anonymously to the S3 Bucket
            fs = s3fs.S3FileSystem(anon=True)

            filelike = io.BytesIO()
            with fs.open(f"{host}/{year}/{month}/{filename}", 'rb') as file:
                with Halo(text=f"Downloading '../{year}/{month}/{filename}' in progress", spinner='dots'):
                    filelike.write(file.read())
                    filelike.seek(0) # Reset the position of the filelike cursor
                print("File downloaded")

            # Load Data in Pandas format
            with Halo(text=f"Convert CSV into Pandas format in progress", spinner='dots'):
                df = pandas.read_csv(filelike)
            print("Data converted into Pandas format")

            if clean:
                with Halo(text=f"Cleaning data in progress", spinner='dots'):
                    # Aggregate duplicated lines and add a new 'count' columns
                    df = df.groupby(df.columns.tolist()).size().to_frame('count').reset_index()
                    df = df.drop_duplicates()
                print(f"Data has been cleaned")

            # Add Data into sqlite DB
            with Halo(text=f"Adding '../{year}/{month}/{filename}' to the DB in progress", spinner='dots'):
                df.to_sql('Events', conn, if_exists='append', index=False)
            print(f"'{year}_{month}.csv' has been added to the DB")

            # Insert downloaded file name in the DB
            cursor.execute("INSERT INTO Files VALUES (?)", (f"{host}/{year}/{month}/{filename}",))
            print(f"'{host}/{year}/{month}/{filename}' has been added into the 'Files' table")
            conn.commit() # Need to commit to save the insert in the DB
            conn.close()

    except PermissionError:
        eprint(f"Error -- You don't have the required permission to access this file")
    except Exception:
        if debug:
            eprint(f"Error -- Exit the program...\n{traceback.print_exception(*sys.exc_info())}")
        else:
            eprint("Error -- Exit the program.")


@main.command()
@click.option('--year', '-y', prompt='Select the year', prompt_required=False, default=f"{current_time.year}", help=f"Format YYYY::{current_time.year}")
@click.option('--month', '-m', prompt='Select the month', prompt_required=False, default=f"{current_time.month:02}", help=f"Format MM::{current_time.month:02}")
@click.option('--host', '-h', default="s3://work-sample-mk", help="S3 path::s3://work-sample-mk")
@click.option('--filename', '-f', default="events.csv", help="File name::events.csv")
@click.option('--debug', '-d', is_flag=True, help="Show error details")
@click.argument('dest', default="")
@click.argument('source', default="")
def downloadFile(year: str, month: str, host: str, filename: str, source: str, dest: str, debug: bool):
    """Just download the target file in local\n
    --year, --month can be passed without value. You will get a prompt which invit you to pass your value.
    If these options are not passed, the default value will be the last month of the current year.\n
    DEST - Specify the destination path and the filename\n
        Default: './{year}_{month}_{filename}'\n
    SOURCE - Specify the source path and the filename\n
        Default: '{host}/{year}/{month}/{filename}'"""


    # Create S3 Object to connect anonymously to the S3 Bucket
    fs = s3fs.S3FileSystem(anon=True)

    if not source:
        source = f"{host}/{year}/{month}/{filename}"

    if not dest:
        dest = f"./{year}_{month}_{filename}"

    # Download and write the targeted file 
    try:
        print(f"Download {source} start")
        with fs.open(source, 'rb') as file:
            out = open(f"{dest}", 'wb')
            with Halo(text=f"Writing '{dest}' in progress", spinner='dots'):
                out.write(file.read())
            print(f"'{dest}' has been written")
    except ValueError:
        eprint(f"{source} if not a valid source")
    except Exception:
        if debug:
            eprint(f"Error -- Year=={year}, Month=={month}\nExit the program...\n{traceback.print_exception(*sys.exc_info())}")
        else:
            eprint(f"Error -- Year=={year}, Month=={month}\nExit the program.")


@main.command()
@click.option('--debug', '-d', default=False, help="Show error details")
@click.argument('dbname', default="Madkudu_events.db")
def testDB(dbname: str, debug: bool):
    """Check if the given database exist\n
    DBNAME - Specify the full path of your targeted DB\n
            Default:'Madkudu_events.db'"""

    try:
        conn = sqlite3.connect(f'file:{dbname}?mode=ro', uri=True)
        print(f"Success -- Connection to the '{dbname}' database established")
        conn.close()
    except sqlite3.OperationalError:
        eprint(f"Failed -- Fail to connect to the '{dbname}' database")
    except Exception:
        if debug:
            eprint(f"Error -- Exit the program...\n{traceback.print_exception(*sys.exc_info())}")
        else:
            eprint("Error -- Exit the program.")


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

if __name__ == "__main__":
    main()
